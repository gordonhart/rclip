import sys
from pathlib import Path

from rclip import db
from rclip import model
from rclip import RClip
from rclip import utils


def main() -> int:
    arg_parser = utils.init_arg_parser()
    args = arg_parser.parse_args()

    current_directory = str(Path(args.search_dir))

    model_instance = model.Model()
    datadir = utils.get_app_datadir()
    database = db.DB(datadir / "db.sqlite3")
    rclip = RClip(model_instance, database, args.exclude_dir)

    if not args.skip_index:
        rclip.ensure_index(current_directory)

    result = rclip.search(args.query, current_directory, args.top)
    if args.filepath_only:
        for r in result:
            print(r.filepath)
    else:
        print("score\tfilepath")
        for r in result:
            print(f"{r.score:.3f}\t'{r.filepath}'")

    return 0


if __name__ == "__main__":
    sys.exit(main())
