import json
import logging
import messybrainz
import os
import tabulate
import uuid

import messybrainz.default_config as config
try:
    import messybraiz.custom_config as config
except ImportError:
    pass

from brainzutils import musicbrainz_db
from messybrainz import db
from messybrainz.db import release as db_release
from sqlalchemy import text

ADMIN_SQL_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "admin", "sql")

# Limit the output of table content to 50 rows, so that we don't end up
# having too much verbose. Can use custom command if complete output is required.
LIMIT = 50

FUNCTIONS = {
    "1": db_release.create_clusters_using_fetched_releases,
    "2": db_release.fetch_and_store_releases_for_all_recording_mbids,
}


def submit_listens(listens):
    """Submits listens to the MessyBrainz database."""

    msb_listens = []
    for listen in listens:
        messy_dict = {
            "artist": listen["artist"],
            "title": listen["title"],
        }
        if "release" in listen:
            messy_dict["release"] = listen["release"]

        if "artist_mbids" in listen:
            messy_dict["artist_mbids"] = listen["artist_mbids"]
        if "release_mbid" in listen:
            messy_dict["release_mbid"] = listen["release_mbid"]
        if "recording_mbid" in listen:
            messy_dict["recording_mbid"] = listen["recording_mbid"]
        msb_listens.append(messy_dict)

    messybrainz.submit_listens_and_sing_me_a_sweet_song(msb_listens)


# lifted from AcousticBrainz
def is_valid_uuid(u):
    try:
        u = uuid.UUID(u)
        return True
    except ValueError:
        return False


def submit_using_json():
    """Submits listens from a give JSON file."""

    filename = input("Enter path to the JSON file: ")
    with open(filename) as f:
        listens = json.load(f)
        submit_listens(listens)
        print("Done!")


def manually_submit_listens():
    """Allows user to manually insert data into database."""

    while True:
        print("Note: you can't leave 'artist_credit', 'recording' empty.")
        artist = input("artist_credit: ")
        while artist == "":
            print("Invalid artist_credit. Try again.")
            artist = input("artist_credit: ")
        title = input("recording: ")
        while title == "":
            print("Invalid recording. Try again.")
            title = input("recording: ")
        release = input("release: ")
        recording_mbid = input("recording_mbid: ")
        while recording_mbid != "" and not is_valid_uuid(recording_mbid):
            print("Invalid UUID inserted. Try again.")
            recording_mbid = input("recording_mbid: ")

        artist_mbids = []
        while True:
            print("Enter artist MBIDs one per line (empty line to end): ")
            artist_mbid = input()
            if(artist_mbid ==""):
                break
            if not is_valid_uuid(artist_mbid):
                print("Invalid UUIDs inserted. Try again.")
                continue
            artist_mbids.append(artist_mbid)

        release_mbid = input("release_mbid: ")
        while release_mbid != "" and not is_valid_uuid(release_mbid):
            print("Invalid UUID inserted. Try again.")
            release_mbid = input("release_mbid: ")

        msb_listen = {"artist": artist, "title": title}
        if release != "":
            msb_listen["release"] = release
        if recording_mbid != "":
            msb_listen["recording_mbid"] = recording_mbid
        if artist_mbids != "":
            msb_listen["artist_mbids"] = artist_mbids
        if release_mbid != "":
            msb_listen["release_mbid"] = release_mbid

        submit_listens([msb_listen])
        print("Done!")

        more = input("Do you want to add more listens(y/n): ")
        if more in ["n", "N", "no", "NO", "No"]:
            break


def add_listens():
    """Allow user to add data to the database with different options."""

    while True:
        input_method = input("Select input method you want to use to submit listens:"
                            "\n0) Go back."
                            "\n1) From JSON file."
                            "\n2) Manually add data.\n"
                        )
        if input_method == "0":
            break
        elif input_method == "1":
            submit_using_json()
        elif input_method == "2":
            manually_submit_listens()
        else:
            print("Enter valid input_method. Try again.")


def reset_db():
    """Resets the database."""

    db.run_sql_script(os.path.join(ADMIN_SQL_DIR, "drop_tables.sql"))
    db.run_sql_script(os.path.join(ADMIN_SQL_DIR, "create_tables.sql"))
    db.run_sql_script(os.path.join(ADMIN_SQL_DIR, "create_primary_keys.sql"))
    db.run_sql_script(os.path.join(ADMIN_SQL_DIR, "create_foreign_keys.sql"))
    db.run_sql_script(os.path.join(ADMIN_SQL_DIR, "create_functions.sql"))
    db.run_sql_script(os.path.join(ADMIN_SQL_DIR, "create_indexes.sql"))
    print("Database reset done.")


def truncate_table():
    """Utility to truncate any table from the database."""

    while True:
        table_name = input("Enter table name to truncate ('0' to go back): ")
        if table_name == "0":
            break
        with db.engine.begin() as connection:
            # This is insecure, but here we are doing it on test_db. So, we can take this risk.
            connection.execute(text("""TRUNCATE TABLE {0} CASCADE""".format(table_name)))
            print("{0} truncated.".format(table_name))


def print_table():
    """Prints the contents of the table user enters. Limits the number of rows to 50."""

    while True:
        table_name = input("Enter table name to print ('0' to go back): ")
        if table_name == "0":
            break
        with db.engine.begin() as connection:
            # This is insecure, but here we are doing it on test_db. So, we can take this risk.
            result = connection.execute(text("""SELECT * FROM {0} LIMIT {1}""".format(table_name, LIMIT)))
            headers = result.keys()
            print(tabulate.tabulate(result, headers=headers, tablefmt="psql"))


def run_script():
    """Runs the script which user wants to test."""

    while True:
        to_run = input("Select option number:\n"
                        "0) Go back\n"
                        "1) create_clusters_using_fetched_releases\n"
                        "2) fetch_and_store_releases_for_all_recording_mbids\n"
                )
        if to_run == "0":
            break
        if to_run not in FUNCTIONS:
            print("Select valid option. Please try again.")
            continue
        else:
            if to_run == "1":
                FUNCTIONS["1"]()
            elif to_run == "2":
                FUNCTIONS["2"]()
            print("Done!")


def set_log_level():
    """Sets the logging level which user desires."""

    while True:
        level_num = input("Enter logging level you want:\n"
                    "0) Go back\n"
                    "1) Debug\n"
                    "2) Info\n"
                    "3) Warning\n"
                    )
        if level_num not in [str(i) for i in range(0,4)]:
            print("Enter valid option number.")
            continue

        logger_level = logging.NOTSET
        if level_num == "0":
            return
        elif level_num == "1":
            logger_level = logging.DEBUG
        elif level_num == "2":
            logger_level = logging.INFO
        elif level_num == "3":
            logger_level = logging.WARNING

        logging.getLogger().setLevel(logger_level)
        print("Logging level set to: {0}".format(logging.getLevelName(logger_level)))
        break


def run_custom_command():
    """Runs custom SQL command entered by user on the database."""

    while True:
        command = input("Enter SQL command ('0' to go back): ")
        if command == "0":
            return
        with db.engine.begin() as connection:
            # This is insecure, but here we are doing it on test_db. So, we can take this risk.
            result = connection.execute(text(command))
            if result.returns_rows:
                headers = result.keys()
                print(tabulate.tabulate(result, headers=headers, tablefmt="psql"))


def test_clustering_manually():
    """Provides interface to an user to test clustering/fetching scripts."""

    try:
        db.init_db_engine(config.TEST_SQLALCHEMY_DATABASE_URI)
        musicbrainz_db.init_db_engine(config.MB_DATABASE_URI)
        logging.basicConfig(format="%(message)s")
    except Exception as e:
        print("While initializing database engine error occured: {0}".format(e))

    while True:
        option = input("Select option number:\n"
                    "0) Exit\n"
                    "1) Select script to run.\n"
                    "2) Add more Listens.\n"
                    "3) Reset database.\n"
                    "4) Truncate table.\n"
                    "5) Print table contents.\n"
                    "6) Change logging level.\n"
                    "7) Run custom SQL command.\n"
                )

        if(option not in [str(i) for i in range(0,8)]):
            print("Invalid option number. Try again.")
            continue
        try:
            if option == "0":
                break
            elif option == "1":
                run_script()
            elif option == "2":
                add_listens()
            elif option == "3":
                reset_db()
            elif option == "4":
                truncate_table()
            elif option == "5":
                print_table()
            elif option == "6":
                set_log_level()
            elif option == "7":
                run_custom_command()
        except Exception as e:
            print("*" * 80)
            print("\nSomething went wrong: {0}".format(e))
            print("*" * 80)

test_clustering_manually()
