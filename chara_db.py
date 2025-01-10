import sqlite3

CREATE_DB_SCRIPT = """
CREATE TABLE IF NOT EXISTS chara_tab(
    chara_name varchar(80) primary key NOT NULL,
    user_ids text NOT NULL default ''
);
"""

def sanitize_chara_name(chara):
    return chara.lower()

class CharaManager:
    def __init__(self, filename):
        self.conn = sqlite3.connect(filename, isolation_level=None)
        self.cur = self.conn.cursor()

    def create_db(self):
        self.cur.execute(CREATE_DB_SCRIPT)

    def migrate_row(self, chara, user_ids_str):
        chara = sanitize_chara_name(chara)

        try:
            res = self.cur.execute("INSERT INTO chara_tab(chara_name, user_ids) VALUES (?, ?)", (chara,user_ids_str))
        except sqlite3.IntegrityError:
            res = self.cur.execute("UPDATE chara_tab set user_ids = ? where chara_name = ?", (user_ids_str, chara))

    def add_chara(self, chara) -> bool:
        try:
            res = self.cur.execute("INSERT INTO chara_tab(chara_name, user_ids) VALUES (?, '')", (chara,))
            return True
        except sqlite3.IntegrityError:
            return False

    def rename_chara(self, old_chara, new_chara):
        old_chara = sanitize_chara_name(old_chara)
        new_chara = sanitize_chara_name(new_chara)

        try:
            res = self.cur.execute("UPDATE chara_tab set chara_name = ? where chara_name = ?", (new_chara, old_chara))
            return True
        except sqlite3.IntegrityError:
            return False

    def add_user_to_chara(self, chara: str, user_id: str):
        chara = sanitize_chara_name(chara)

        res = self.cur.execute("SELECT user_ids from chara_tab where chara_name = ?", (chara,))
        x = res.fetchone()
        if not x:
            return False
        user_ids = x[0]
        if user_ids == "":
            user_ids = []
        else:
            user_ids = user_ids.split(",")
        if user_id in user_ids:
            return False
        user_ids.append(user_id)

        self.cur.execute("UPDATE chara_tab set user_ids=? where chara_name = ?", (",".join(user_ids), chara))
        return True

    def remove_user_to_chara(self, chara: str, user_id: str):
        chara = sanitize_chara_name(chara)

        res = self.cur.execute("SELECT user_ids from chara_tab where chara_name = ?", (chara,))
        x = res.fetchone()
        if not x:
            return False
        user_ids = x[0]
        if user_ids == "":
            user_ids = []
        else:
            user_ids = user_ids.split(",")
        if user_id not in user_ids:
            return False
        user_ids.remove(user_id)

        self.cur.execute("UPDATE chara_tab set user_ids=? where chara_name = ?", (",".join(user_ids), chara))
        return True

    def get_charas_for_user(self, user_id):
        rows = self.getall()
        ans = []
        for row in rows:
            if user_id in row[1].split(","):
                ans.append(row[0])
        return ans
    
    def get_charas(self):
        res = self.cur.execute("SELECT chara_name from chara_tab")
        rows = res.fetchall()
        return [r[0] for r in rows]

    def getall(self):
        res = self.cur.execute("SELECT * from chara_tab")
        rows = res.fetchall()
        return rows

    def listall(self):
        for row in self.getall():
            print(row)

def test():
    charaManager = CharaManager("test.db")
    charaManager.create_db()
    charaManager.add_chara("oniwaka")
    charaManager.add_chara("orgus")
    charaManager.add_user_to_chara("oniwaka", "12345")
    charaManager.add_user_to_chara("orgus", "12345")
    charaManager.add_user_to_chara("oniwaka", "456")
    charaManager.add_user_to_chara("oniwaka", "789")
    charaManager.add_user_to_chara("xxx", "12345")
    charaManager.remove_user_to_chara("oniwaka", "789")
    charaManager.remove_user_to_chara("oniwaka", "1")
    charaManager.listall()
    print(charaManager.get_charas_for_user("12345"))

if __name__ == '__main__':
    test()
