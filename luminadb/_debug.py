"""Used for debugging"""

from inspect import getouterframes, FrameInfo, Traceback

STATE = {"DEBUG": False}


def _build_frame_str(frame: FrameInfo | Traceback):
    context_lines = (
        "<no context>\n"
        if not frame.code_context
        else "\n>>> ".join(frame.code_context)
    )
    lineno = frame.lineno or "<None>"
    filename = frame.filename or "<None>"
    function = frame.function or "<Callable()>"
    return f"""\
Trace at {lineno}, file {filename}, {function}
>>> {context_lines}"""


def map_frame(frame: Traceback) -> str:
    """Map a Taceback or FrameInfo to readable trace"""
    allframes = getouterframes(frame)
    allframes.reverse()

    builded = [_build_frame_str(frame) for frame in allframes]
    return "".join(builded)


def map_frames(frames: list[FrameInfo | Traceback]):
    """Shorthand for alreadby built list of frames"""
    return "".join((_build_frame_str(frame) for frame in frames))


def if_debug_print(*args, sep=" ", end="\n", flush=True):
    """If debug? print!"""
    if STATE["DEBUG"]:
        arg0 = args[0]
        print(
            arg0,
            *(repr(arg) if arg != "\n" else "\n" for arg in args if args.index(arg) != 0),
            sep=sep,
            end=end,
            flush=flush,
        )
