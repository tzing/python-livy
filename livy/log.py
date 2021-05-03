import datetime
import hashlib
import logging
import re
import typing

import livy.client
import livy.exception

__all__ = ["LivyBatchLogReader"]

logger = logging.getLogger(__name__)

DEFAULT_LEVEL = {
    "stdout": logging.INFO,
    "stderr": logging.ERROR,
    "YARN Diagnostics": logging.WARNING,
}


class LivyLogParseResult(typing.NamedTuple):
    """Log parse result."""

    created: datetime.datetime
    """Timestamp that the log is created. Could be None if we could not determine
    when does it created, would use the time last log is created.
    """

    level: int
    """Log level.
    """

    name: str
    """Logger name. Could be None if unknown, would be fallback to corresponding
    output stream name (stdout/stderr).
    """

    message: str
    """Log message.
    """


LivyLogParser = typing.Callable[[typing.Match], LivyLogParseResult]


class LivyBatchLogReader:
    """Read Livy batch logs and publish to Python's logging infrastructure."""

    _parsers: typing.Dict[typing.Pattern, LivyLogParser]

    def __init__(
        self,
        client: livy.client.LivyClient,
        batch_id: int,
        timezone: datetime.tzinfo = datetime.timezone.utc,
        prefix: str = None,
    ) -> None:
        """
        Parameters
        ----------
            client : livy.LivyClient
                Livy client that is pre-configured
            batch_id : int
                Batch ID to be watched
            timezone : datetime.tzinfo
                Server time zone
            prefix : str
                Prefix to be added to logger name

        Raises
        ------
        TypeError
            On a invalid data type is used for inputted argument
        """
        if not isinstance(client, livy.client.LivyClient):
            raise livy.exception.TypeError("client", livy.client.LivyClient, client)
        if not isinstance(batch_id, int):
            raise livy.exception.TypeError("batch_id", int, batch_id)
        if not isinstance(timezone, datetime.tzinfo):
            raise livy.exception.TypeError("timezone", datetime.tzinfo, timezone)
        if prefix and not isinstance(prefix, str):
            raise livy.exception.TypeError("prefix", str, prefix)

        self.client = client
        self.batch_id = batch_id
        self.timezone = timezone
        self.prefix = prefix or ""

        self._section_match = object()
        self._parsers = {
            # indicator that the section is changed
            re.compile(
                "^(stdout|stderr|YARN Diagnostics): ", re.RegexFlag.MULTILINE
            ): self._section_match,
            # default parser
            re.compile(
                r"^(\d{2}\/\d{2}\/\d{2} \d{2}:\d{2}:\d{2}) ([A-Z]+) (.+?):(.*(?:\n\t.+)*)\n",
                re.RegexFlag.MULTILINE,
            ): default_parser,
        }

        self._emitted_logs = set()
        self._last_emit_timestamp = None

    def add_parsers(
        self,
        pattern: typing.Pattern,
        parser: LivyLogParser,
    ):
        """Add log parser to this reader.

        Parameters
        ----------
            pattern : re.Pattern
                Regex pattern to match the log. Note the pattern must match
                line start (^) symbol and compiled with multiline (M) flag.
            parser : callable
                The parser to extract fields from the re.Match object. It should
                take re.Match as input and returns LivyLogParseResult instance.

        Return
        ------
        No return. It raises exception on any error.

        Note
        ----
        The pattern must wrapped entire log
            The re.Match object is also used to locate the position in the log.
            It might cause error if the regex pattern does not match entire log
            lines.

        Do not directly emit log from the parser
            The fetched log might have overlap with previous action, this reader
            does cached the processed result and prevent duplicated logs emitted.
        """
        if not isinstance(pattern, typing.Pattern):
            raise livy.exception.TypeError("pattern", "regex pattern", pattern)
        if not callable(parser):
            raise livy.exception.TypeError("parser", "callable", parser)
        self._parsers[pattern] = parser

    def read(self):
        """Read log once.

        Return
        ------
        No return. All logs would be pipe to Python's `logging` library.

        Note
        ----
        Livy does not split logs into different object or something. What we
        could be reterived from server is a list of string that mixed with
        outputs in stdout and stderr.

        This function is degned to match through each of known format via regex,
        and fallback to stdout/stderr if we could not parse it.

        Parsers are pluggable. Beyond the builtin parsers, read instruction from
        docstring of `add_parser`.
        """
        # get log
        # use size -1 to get as much log as possible
        logs = self.client.get_batch_log(self.batch_id, size=-1)
        logs = "\n".join(logs)

        # initial matching
        matches: typing.Dict[typing.Pattern, typing.Match] = {}
        for pattern in self._parsers:
            m = pattern.search(logs)
            if not m:
                continue
            matches[pattern] = m

        # iter through complete log
        pos = 0
        current_section = "stdout"
        while pos < len(logs):
            # match recent text
            pos, match, parser = self._match_log(matches, logs, pos)

            # special case: change section name
            if parser is self._section_match:
                current_section = match.group(1)
                continue

            if not match:
                continue

            # parse log
            try:
                result = parser(match)
            except:
                logger.exception(
                    "Error during parsing log in %s. Raw match=%s", parser, match
                )
                continue

            # cache for preventing emit duplicated logs
            digest = hashlib.md5(
                b"%f--%d--%d--%d"
                % (
                    result.created.timestamp() if result.created else 0,
                    result.level,
                    hash(result.name),
                    hash(result.message),
                )
            ).digest()

            if digest in self._emitted_logs:
                continue
            else:
                self._emitted_logs.add(digest)

            # emit
            name = result.name
            level = result.level
            if not name:
                name = current_section
                level = DEFAULT_LEVEL[current_section]

            created = result.created
            if not created:
                created = self._last_emit_timestamp or datetime.datetime.now()
            else:
                self._last_emit_timestamp = created
            if not created.tzinfo:
                created = created.replace(tzinfo=self.timezone)

            record = logging.makeLogRecord(
                {
                    "name": self.prefix + name,
                    "levelno": level,
                    "levelname": logging.getLevelName(result.level),
                    "msg": result.message,
                    "created": created,
                }
            )

            logging.getLogger(record.name).handle(record)

    def _match_log(
        self, matches: typing.Dict[typing.Pattern, typing.Match], logs: str, pos: int
    ) -> typing.Tuple[int, typing.Match, LivyLogParser]:
        """Helper function to match most-recent text and get corresponding
        parser.

        Parameters
        ----------
            matches : Dict[re.Pattern, re.Match]
                Cached match object dict for accelerate searching
            logs : str
                Complete log context
            pos : int
                Current position indicator for searching and parsing

        Returns
        -------
            new_pos : int
                New position indicator after this match
            match : re.Match
                Current matched object
            parser : LivyLogParser
                Parser for current match
            (matches) : Dict[re.Pattern, re.Match]
                Cached match object dict, updated in-place
        """
        if not matches:
            # some text remained but no pattern matched
            # flush all to fallback logger
            match = logs[pos:]
            new_pos = len(logs)
            parser = convert_stdout
            return new_pos, match, parser

        # get match that is neatest to current pos
        pattern, match = min(matches.items(), key=lambda x: x[1].start())

        if match.start() == pos:
            new_pos = match.end()
            parser = self._parsers[pattern]
        else:
            new_pos = match.start()
            # trick: `convert_stdout` takes str for input
            match = logs[pos : match.start()].strip()
            parser = convert_stdout

        # find next match
        next_match = pattern.search(logs, new_pos)
        if next_match:
            matches[pattern] = next_match
        else:
            del matches[pattern]

        return new_pos, match, parser


def default_parser(match: typing.Match):
    """Parser for default PySpark log format."""
    LEVEL_MAPPING = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARN": logging.WARNING,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "FATAL": logging.CRITICAL,
        "CRITICAL": logging.CRITICAL,
    }

    time = datetime.datetime.strptime(match.group(1), "%y/%m/%d %H:%M:%S")
    level = LEVEL_MAPPING.get(match.group(2), logging.CRITICAL)

    return LivyLogParseResult(
        created=time,
        level=level,
        name=match.group(3),
        message=match.group(4).lstrip().replace("\n\t", "\n"),
    )


def convert_stdout(text: str):
    """Convert stdout (and stderr) text to parse result object."""
    return LivyLogParseResult(
        created=None,
        level=logging.INFO,
        name=None,
        message=text.strip(),
    )
