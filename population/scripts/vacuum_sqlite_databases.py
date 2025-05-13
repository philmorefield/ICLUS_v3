import glob
import os
import sqlite3


def main():
    p = 'D:\\OneDrive'

    for db in glob.glob(os.path.join(p, '**\\*.sqlite'), recursive=True):
        if 'ICLUS_v3' in db:
            continue
        print(f"Vacuuming {db}...")
        con = sqlite3.connect(db)
        con.execute("VACUUM")
        con.close()

    print("Finished!")

if __name__ == '__main__':
    main()
