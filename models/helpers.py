def printr(text:str):
    """Prints the given text in red color."""
    print("\033[91m{}\033[0m".format(text))

def printy(text:str):
    """Prints the given text in yellow color."""
    print("\033[93m{}\033[0m".format(text))

def printg(text:str):
    """Prints the given text in green color."""
    print("\033[92m{}\033[0m".format(text))

def printb(text: str):
    """Prints the given text in blue color."""
    print("\033[94m{}\033[0m".format(text))

def printc(text: str):
    """Prints the given text in cyan color."""
    print("\033[36m{}\033[0m".format(text))