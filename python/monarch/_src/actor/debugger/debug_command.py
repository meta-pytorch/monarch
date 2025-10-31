# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict
import sys
from dataclasses import dataclass
from typing import cast, Dict, List, Optional, Tuple, TYPE_CHECKING, Union

if TYPE_CHECKING:
    from lark import Lark, Transformer

from monarch._src.actor.debugger.debug_io import DebugIO

RanksType = Union[int, List[int], range, Dict[str, Union[range, List[int], int]]]

_debug_input_parser: "Optional[Lark]" = None


# Wrap the parser in a function so that jobs don't have to import lark
# unless they want to use the debugger.
def _get_debug_input_parser() -> "Lark":
    global _debug_input_parser
    if _debug_input_parser is None:
        from lark import Lark

        _debug_input_parser = Lark(
            """
            rank_list: INT "," INT ("," INT)*
            start: INT?
            stop: INT?
            step: INT?
            rank_range: start ":" stop (":" step)?
            dim: CNAME "=" (rank_range | "(" rank_list ")" | INT)
            dims: dim ("," dim)*
            ranks: "ranks(" (dims | rank_range | rank_list | INT) ")"
            pdb_command: /\\w+.*/
            actor_name: /[-_a-zA-Z0-9]+/
            cast: "cast" _WS actor_name ranks pdb_command
            help: "h" | "help"
            attach: ("a" | "attach") _WS actor_name INT
            cont: "c" | "continue"
            quit: "q" | "quit"
            list: "l" | "list"
            command: attach | list | cast | help | cont | quit

            _WS: WS+

            %import common.INT
            %import common.CNAME
            %import common.WS
            %ignore WS
            """,
            start="command",
        )
    return _debug_input_parser


_debug_input_transformer: "Optional[Transformer]" = None


# Wrap the transformer in a function so that jobs don't have to import lark
# unless they want to use the debugger.
def _get_debug_input_transformer() -> "Transformer":
    global _debug_input_transformer
    if _debug_input_transformer is None:
        from lark import Transformer
        from lark.lexer import Token

        class _IntoDebugCommandTransformer(Transformer):
            def rank_list(self, items: List[Token]) -> List[int]:
                return [int(item.value) for item in items]

            def start(self, items: List[Token]) -> int:
                if len(items) == 0:
                    return 0
                return int(items[0].value)

            def stop(self, items: List[Token]) -> int:
                if len(items) == 0:
                    return sys.maxsize
                return int(items[0].value)

            def step(self, items: List[Token]) -> int:
                if len(items) == 0:
                    return 1
                return int(items[0].value)

            def rank_range(self, items: List[int]) -> range:
                return range(*items)

            def dim(
                self, items: Tuple[Token, Union[range, List[int], Token]]
            ) -> Tuple[str, Union[range, List[int], int]]:
                if isinstance(items[1], range):
                    return (items[0].value, cast(range, items[1]))
                elif isinstance(items[1], list):
                    return (items[0].value, cast(List[int], items[1]))
                else:
                    return (items[0].value, int(cast(Token, items[1]).value))

            def dims(
                self, items: List[Tuple[str, Union[range, List[int], int]]]
            ) -> Dict[str, Union[range, List[int], int]]:
                return {dim[0]: dim[1] for dim in items}

            def ranks(self, items: List[Union[RanksType, Token]]) -> RanksType:
                if isinstance(items[0], Token):
                    return int(cast(Token, items[0]).value)
                return cast(RanksType, items[0])

            def pdb_command(self, items: List[Token]) -> str:
                return items[0].value

            def actor_name(self, items: List[Token]) -> str:
                return items[0].value

            def help(self, _items: List[Token]) -> "Help":
                return Help()

            def attach(self, items: Tuple[str, Token]) -> "Attach":
                return Attach(items[0], int(items[1].value))

            def cont(self, _items: List[Token]) -> "Continue":
                return Continue()

            def quit(self, _items: List[Token]) -> "Quit":
                return Quit()

            def cast(self, items: Tuple[str, RanksType, str]) -> "Cast":
                return Cast(*items)

            def list(self, items: List[Token]) -> "ListCommand":
                return ListCommand()

            def command(self, items: List["DebugCommand"]) -> "DebugCommand":
                return items[0]

        _debug_input_transformer = _IntoDebugCommandTransformer()
    return _debug_input_transformer


class DebugCommand:
    @staticmethod
    async def parse(debug_io: DebugIO, line: str) -> Union["DebugCommand", None]:
        try:
            tree = _get_debug_input_parser().parse(line)
            return _get_debug_input_transformer().transform(tree)
        except Exception as e:
            await debug_io.output(f"Error parsing input: {e}\n")
            return None


@dataclass
class Attach(DebugCommand):
    actor_name: str
    rank: int


@dataclass
class ListCommand(DebugCommand):
    pass


@dataclass
class Quit(DebugCommand):
    pass


@dataclass
class Help(DebugCommand):
    pass


@dataclass
class Continue(DebugCommand):
    pass


@dataclass
class Cast(DebugCommand):
    actor_name: str
    ranks: RanksType
    command: str
