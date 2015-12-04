from flask import Flask, request, g
import time
from FlaskDBHelper import DBHelper

DATABASE = 'database.db'
db = DBHelper(DATABASE, g, True)

class Consolidator():
    def looper(self):
        while True:

            pos_1 = db.query('position1')[-1]
            pos_2 = db.query('position2')[-1]
            temperatura = db.query('temperature')[-1]
            gas = db.query('gas')[-1]
            queda = db.query('queda')[-1]

            result = db.insert('consolidated',
                               ['pos_1', 'pos_2', 'temperatura', 'queda', 'gas'],
                               [pos_1, pos_2, temperatura, queda, gas])
            time.sleep(2)

if __name__ == "__main__":
    cons = Consolidator()
    cons.looper()