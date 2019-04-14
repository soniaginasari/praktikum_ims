import pymysql
import time


def enginebank():
    #query untuk select tabel transaksi dan sinkronisasi
    selectTransaksi = "SELECT * FROM tb_transaksi"
    selectSinkron = "SELECT * FROM tb_sinkronisasi"

    # mengambil semua data pada tabel transaksi dan memasukannya pada variabel dataTBank
    curBank.execute(selectTransaksi)
    dataTBank = curBank.fetchall()

    # mengambil semua data pada tabel sinkronisasi dan memasukannya pada variabel data
    curBank.execute(selectSinkron)
    dataSBank = curBank.fetchall()

    # mengambil data yang tidak ada di tabel sinkronisasi
    selectData = """SELECT id_transaksi, nama, uang, status FROM tb_transaksi
                WHERE id_transaksi NOT IN (SELECT id_transaksi FROM tb_sinkronisasi);"""

    # mengambil data yang dihapus
    selectDataDel = """SELECT id_transaksi FROM tb_sinkronisasi 
                    WHERE id_transaksi NOT IN (SELECT id_transaksi FROM tb_transaksi)"""

    # membandingkan banyak data di kedua tabel
    if(len(dataTBank)) > (len(dataSBank)):
        print("Ada data yang ditambahkan pada tb_transaksi")
        curBank.execute(selectData)
    #     melakukan iterasi untuk mendapatkan data yang input data ke tb_sinkronisasi
        for data in curBank.fetchall():
            # insert data ke tb_sinkronisasi
            curBank.execute("INSERT INTO tb_sinkronisasi(id_transaksi, nama, uang, status) VALUES(%s,%s,%s,%s)", (data[0], data[1], data[2], data[3]))
            conBank.commit()
            print("data berhasil ditambahkan")
    elif(len(dataTBank)) < (len(dataSBank)):
        print("Ada data transaksi yang dihapus")
        curBank.execute(selectDataDel)
        # melakukan iterasi untuk mendapatkan data yang didelete
        for data in curBank.fetchall():
            # delete data di tb_sinkronisasi
            curBank.execute("DELETE FROM tb_sinkronisasi WHERE id_transaksi = %s", (data[0]))
            conBank.commit()
            print("data berhasil di delete")
    else :
        print("tidak ada perubahan data pada tabel transaksi")



def enginetoko():
    # query untuk select tabel transaksi dan sinkronisasi
    selectTransaksi = "SELECT * FROM tb_transaksi"
    selectSinkron = "SELECT * FROM tb_sinkronisasi"

    # mengambil semua data pada tabel transaksi dan memasukannya pada variabel dataTBank
    curToko.execute(selectTransaksi)
    dataTToko = curToko.fetchall()

    # mengambil semua data pada tabel sinkronisasi dan memasukannya pada variabel data
    curToko.execute(selectSinkron)
    dataSToko = curToko.fetchall()

    # mengambil data yang tidak ada di tabel sinkronisasi
    selectData = """SELECT id_transaksi, nama, uang, status FROM tb_transaksi
                   WHERE id_transaksi NOT IN (SELECT id_transaksi FROM tb_sinkronisasi);"""

    # mengambil data yang dihapus
    selectDataDel = """SELECT id_transaksi FROM tb_sinkronisasi 
                       WHERE id_transaksi NOT IN (SELECT id_transaksi FROM tb_transaksi)"""

    # membandingkan banyak data di kedua tabel
    if (len(dataTToko)) > (len(dataSToko)):
        print("Ada data yang ditambahkan pada tb_transaksi")
        curToko.execute(selectData)
        #     melakukan iterasi untuk mendapatkan data yang input data ke tb_sinkronisasi
        for data in curToko.fetchall():
            # insert data ke tb_sinkronisasi
            curToko.execute("INSERT INTO tb_sinkronisasi(id_transaksi, nama, uang, status) VALUES(%s,%s,%s,%s)",
                            (data[0], data[1], data[2], data[3]))
            conToko.commit()
            print("data berhasil ditambahkan")
    elif (len(dataTToko)) < (len(dataSToko)):
        print("Ada data transaksi yang dihapus")
        curToko.execute(selectDataDel)
        # melakukan iterasi untuk mendapatkan data yang didelete
        for data in curToko.fetchall():
            # delete data di tb_sinkronisasi
            curToko.execute("DELETE FROM tb_sinkronisasi WHERE id_transaksi = %s", (data[0]))
            conToko.commit()
            print("data berhasil di delete")
    else:
        print("tidak ada perubahan data pada tabel transaksi")


def sinkronisasi():
        # query untuk select tabel transaksi dan sinkronisasi
        selectSinkron = "SELECT * FROM tb_sinkronisasi"

        # mengambil semua data pada tabel transaksi dan memasukannya pada variabel dataTBank
        curToko.execute(selectSinkron)
        curBank.execute(selectSinkron)

        # mendapatkan semua data pada tabel sinkronisasi db_bank dan db_toko
        dataSToko = curToko.fetchall()
        dataSBank = curBank.fetchall()

        # mengambil semua data pada tabel sinkronisasi dan memasukannya pada variabel data
        curToko.execute(selectSinkron)
        dataSToko = curToko.fetchall()

        # mengambil data yang tidak ada di tabel sinkronisasi
        selectData = """SELECT id_transaksi, nama, uang, status FROM db_toko.tb_sinkronisasi
                       WHERE id_transaksi NOT IN (SELECT id_transaksi FROM db_bank.tb_sinkronisasi);"""

        # mengambil data yang dihapus
        selectDataDel = """SELECT id_transaksi FROM db_bank.tb_sinkronisasi 
                           WHERE id_transaksi NOT IN (SELECT id_transaksi FROM db_toko.tb_transaksi)"""

        # membandingkan banyak data di kedua tabel
        if (len(dataSToko)) > (len(dataSBank)):
            print("Ada data yang ditambahkan pada tb_transaksi")
            curToko.execute(selectData)
            #     melakukan iterasi untuk mendapatkan data yang input data ke tb_sinkronisasi
            for data in curToko.fetchall():
                # insert data ke tb_sinkronisasi
                curToko.execute("""INSERT INTO db_bank.tb_transaksi(id_transaksi, nama, uang, status) 
                                VALUES(%s,%s,%s,%s)""",
                                (data[0], data[1], data[2], data[3]))
                conToko.commit()
                print("data berhasil ditambahkan")
        elif (len(dataSToko)) < (len(dataSBank)):
            print("Ada data transaksi yang dihapus")
            curToko.execute(selectDataDel)
            # melakukan iterasi untuk mendapatkan data yang didelete
            for data in curToko.fetchall():
                # delete data di tb_sinkronisasi
                curToko.execute("DELETE FROM db_bank.tb_transaksi WHERE id_transaksi = %s", (data[0]))
                conToko.commit()
                print("data berhasil di delete")
        else:
            print("tidak ada perubahan data pada tabel transaksi")


while True:
    try:
        # koneksi dengan databse bank
        conToko = pymysql.connect('localhost', 'root', '', 'db_toko')
        conBank = pymysql.connect('localhost', 'root', '', 'db_bank')
        # membuat cursor untuk mengeksekusi query
        curToko = conToko.cursor()
        curBank = conBank.cursor()


        # memanggil engine toko
        enginetoko()


        # memanggil engine bank
        enginebank()


        sinkronisasi()

        # memanggil engine

    except:
        print("koneksi gagal")
    time.sleep(5) #memberikan waktu delay selama 5 detik
