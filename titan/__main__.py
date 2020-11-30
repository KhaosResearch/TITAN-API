import argparse

from titan import __author__, __version__

HEADER = "\n".join(
    [
        r"  _____ ___ _____  _    _   _ ",
        r" |_   _|_ _|_   _|/ \  | \ | |",
        r"   | |  | |  | | / _ \ |  \| |",
        r"   | |  | |  | |/ ___ \| |\  |",
        r"   |_| |___| |_/_/   \_\_| \_|",
        "                                                    ",
        f" ver. {__version__}     author {__author__}        ",
        "                                                    ",
    ]
)


def get_parser():
    parser = argparse.ArgumentParser(prog="TITAN PLATFORM")

    subparsers = parser.add_subparsers(dest="command", help="TITAN sub-commands")
    subparsers.required = True

    subparsers.add_parser("server", help="Deploy server")

    return parser


def cli():
    print(HEADER)
    args, _ = get_parser().parse_known_args()

    if args.command == "server":
        from .app import run_server

        run_server()


if __name__ == "__main__":
    cli()
