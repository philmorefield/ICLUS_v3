import glob
import os
import sqlite3


def main():
    p = 'D:\\OneDrive\\ICLUS_v3\\population'

    for db in glob.glob(os.path.join(p, '**\\*.sqlite'), recursive=True):
        con = sqlite3.connect(db)
        con.execute("VACUUM")
        con.close()

    print("Finished!")

if __name__ == '__main__':
    main()
