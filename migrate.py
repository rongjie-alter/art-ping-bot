import csv
import urllib.request
import argparse

import chara_db

MAX_ID_PER_MSG = 2000 // (4 + 18)
MAX_CACHE_SIZE = 300
CSV_CACHE_SECOND = 10 * 60

HOUSAMO_SHEET = "https://docs.google.com/spreadsheets/d/1nr4k_-5DKjgyCX49gokg5iPpQ9a5l3d0lGrGnrnDR2U/gviz/tq?tqx=out:csv&sheet=Sheet1&headers=0"
LAH_SHEET = "https://docs.google.com/spreadsheets/d/1xKhpSCMeyATJMr6Ur8q_OcvYEMPXYRJoX5hfrNOwSKU/gviz/tq?tqx=out:csv&sheet=Sheet1&headers=0"
EIDOS_SHEET = "https://docs.google.com/spreadsheets/d/1ycqCALRsh2f6aoIeI4EjlFL5bZjVNlQerQfFLhsRems/gviz/tq?tqx=out:csv&sheet=Sheet1&headers=0"


CONFIG = {
    # samocord
    280061796390404096: {
        "channels": [420080593217257487, 285588282522206208, 698643025236066426, 698701417971712030, 844714468390731777],
        "sheet": HOUSAMO_SHEET,
        "filename": "housamo.csv",
        "db_filename": "housamo.db",
    },
    # herocord
    758267927362797568: {
        "channels": [758267927878828046, 758267927878828047, 758267927878828044, 758267927878828045, 844972742692700240],
        "sheet": LAH_SHEET,
        "filename": "lah.csv",
        "db_filename": "lah.db",
    },
    # eidos
    904187573696618506: {
        "channels": [904207190561267794, 904519401699700746, 904224270945775648],
        "sheet": EIDOS_SHEET,
        "filename": "eidos.csv",
        "db_filename": "eidos.db",
    },
}

def read_csv(filename, dbname):
    charaManager = chara_db.CharaManager(dbname)
    charaManager.create_db()

    with open(filename, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            chara = row[0]
            ids = ",".join(row[1].split())
            charaManager.migrate_row(chara, ids)

def download_csv(sheet, filename, guild):
    req = urllib.request.Request(sheet)
    with urllib.request.urlopen(req) as res:
        with open(filename, "wb") as f:
            f.write(res.read())

    guild["data"] = read_csv(filename, guild["db_filename"])

def download_impl(args):
    for c in CONFIG.values():
        download_csv(c["sheet"], c["filename"], c)

def print_impl(args):
    for c in CONFIG.values():
        charaManager = chara_db.CharaManager(c["db_filename"])
        print(f">>>> {c['db_filename']}")
        charaManager.listall()
        print()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(required=True)

    parser_download = subparsers.add_parser('download', add_help=True, help="Download from gsheet")
    parser_download.set_defaults(func=download_impl)

    parser_print = subparsers.add_parser('print', add_help=True, help="Print local db")
    parser_print.set_defaults(func=print_impl)

    args = parser.parse_args()
    args.func(args)
