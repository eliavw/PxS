def debug_print(msg, level=1, V=0):
    """
    Print a debug message.

    This is a useful method because it listens to the verbosity. You can set
    a level to this statement, which will be compared with the verbosity

    Parameters
    ----------
    msg: str
        The thing you sometimes want to see.
    level: int
        The `level` of importance of the message. Messages will only be printed
        if the VERBOSITY is equal or higher than their level.
    V: int
        Verbosity of the program.

    Returns
    -------

    """

    if V >= level:
        print(msg+"\n")
    return