import pymysql #import modul pymysql untuk melakukan koneksi ke database
import time #import modul time untuk waktu jeda manual

#Deklarasi Fungsi engineToko
def engineToko():
    #QWERY Select Semua Data di tb_transaksi dan tb_integrasi
    selectTransaksi="""SELECT * FROM tb_transaksi;"""
    selectIntegrasi = """SELECT * FROM tb_integrasi;"""

    #Mengambil Data di Tabel Transaksi
    curToko.execute(selectTransaksi)
    dataToko = curToko.fetchall() #Memasukan Semua Data ke variabel 'dataToko'

    # Mengambil Data di Tabel Transaksi
    curToko.execute(selectIntegrasi)
    dataIntegrasi = curToko.fetchall()#Memasukan Semua Data ke variabel 'dataIntegrasi'

    #QWERY mengambil data pada tb_transaksi yang tidak ada di tb_integrasi
    selectData = """SELECT id_transaksi, id_pembeli, tgl_transaksi, total_transaksi, status FROM tb_transaksi
                    WHERE id_transaksi NOT IN(SELECT id_transaksi FROM tb_integrasi);"""
    #QWERY mengambil data pada tb_integrasi yang tidak ada di tb_transaksi
    selectData2 = """SELECT id_transaksi FROM tb_integrasi
                     WHERE id_transaksi NOT IN(SELECT id_transaksi FROM tb_transaksi)"""
    #Singkronisasi Data tb_transaksi dengan tb_integrasi di Toko
    if (len(dataToko) > len(dataIntegrasi)) :   #membandingkan banyak data di tb_toko dan tb_integrasi
        print("Ada Data Baru Ditambahkan pada Transaksi Toko!")
        curToko.execute(selectData)
        #proses menambahkan data ke tabel tb_integrasi
        for data in curToko.fetchall():
            #eksekusi QWERY insert ke tb_integrasi
            curToko.execute("""INSERT INTO tb_integrasi(id_transaksi, id_pembeli, tgl_transaksi, nominal_transaksi, status) 
                                VALUES(%s, %s, %s, %s, %s)""", (data[0], data[1], data[2], data[3], data[4]))
            connToko.commit() #sintak untuk menyinpan perubahan pada database toko
            print("Data Berhasil di-Update")

    # Singkronisasi Data tb_integrasi dengan tb_transaksi di Toko
    elif (len(dataIntegrasi) > len(dataToko)): #membandingkan banyak data di tb_integrasi dan tb_toko
        print("Ada Data yang Dihapus di Transaksi Toko!")
        curToko.execute(selectData2)
        # proses menghapus data pada tabel tb_integrasi
        for data in curToko.fetchall():
            #eksekusi QWERY delete ke tb_integrasi
            curToko.execute("""DELETE FROM tb_integrasi WHERE id_transaksi = %s""", data)
            connToko.commit()#sintak untuk menyinpan perubahan pada database toko
            print("Data Berhasil di-Update")
    else :
        print("Tidak Ada Data Baru di Transaksi Toko")

#Deklarasi Fungsi engineBank
def engineBank():
    print("Engine Toko") #Belom jadi hehehe

#Deklarasi Fungsi engineToko
def engineSingkronisasi():
    #QWERY select semua data di tb_integrasi
    selectIntegrasi = """SELECT * FROM tb_integrasi;"""
    #eksekusi QWERY diatas tadi dengan 'cursor' toko
    curToko.execute(selectIntegrasi)
    #memasukan semua nilai di QWERY yang baru dijalankan dengan 'cursor' toko ke variabel
    integrasiToko = curToko.fetchall()
    # eksekusi QWERY diatas tadi dengan 'cursor' bank
    curBank.execute(selectIntegrasi)
    # memasukan semua nilai di QWERY yang baru dijalankan dengan 'cursor' bank ke variabel
    integrasiBank = curBank.fetchall()
    #QWERY mengambil semua data pada tb_integrasi toko yang tidak ada di tb_integrasi bank
    selectData = """SELECT * FROM db_toko.tb_integrasi
                    WHERE id_transaksi NOT IN(SELECT db_bank.tb_integrasi.id_transaksi FROM db_bank.tb_integrasi);"""
    # QWERY mengambil semua data pada tb_integrasi bank yang tidak ada di tb_integrasi toko
    selectData2 = """SELECT db_bank.tb_integrasi.id_transaksi FROM db_bank.tb_integrasi
                     WHERE id_transaksi NOT IN(SELECT db_toko.tb_integrasi.id_transaksi FROM db_toko.tb_integrasi)"""
    #singkronisasi tb_integrasi toko dengan tb_integrasi bank
    if (len(integrasiToko) > len(integrasiBank)):  #membandingkan banyak data di tb_integrasi toko dan tb_integrasi bank
        print("Ada Data Baru Ditambahkan ke Integrasi Toko")
        curBank.execute(selectData)
        #proses menambahkan data ke tb_integrasi bank
        for data in curBank.fetchall():
            #eksekusi QWERY insert ke tb_integrasi bank dengan 'cursor' bank
            curBank.execute("""INSERT INTO tb_integrasi 
                               VALUES(%s, %s, %s, %s, %s, %s)""", (data[0], data[1], data[2], data[3], data[4], data[5]))
            connBank.commit()  #sintak untuk menyimpan perubahan pada database bank
            print("Data Berhasil di-Update")
    #singkronisasi tb_integrasi bank dengan tb_integrasi toko
    elif (len(integrasiBank) > len(integrasiToko)): #membandingkan banyak data di tb_integrasi bank dan tb_integrasi toko
        print("Ada Data yang Dihapus di Integrasi Toko")
        curBank.execute(selectData2)
        #proses menghapus data pada tb_integrasi bank
        for data in curBank.fetchall():
            #eksekusi QWERY delete di tb_integrasi bank dengan 'cursor' bank
            curBank.execute("""DELETE FROM tb_integrasi WHERE id_transaksi = %s""", data[0])
            connBank.commit() #sintak untuk menyimpan perubahan pada database bank
            print("Data Berhasil di-Update")
    else:
        print("Tidak Ada Perubahan pada Tabel Integrasi")
while(1):  # perulangan tak hingga
    try: # coba klo berhasil jalankan
        #Connect ke Database Toko dan Bank
        connToko = pymysql.connect(host='localhost', user='root', passwd='', db='db_toko')
        connBank = pymysql.connect(host='localhost', user='root', passwd='', db='db_bank')
        #Buat Cursor untuk jalanin QWERY di Database
        curToko = connToko.cursor()
        curBank = connBank.cursor()

        #Memanggin Fungsi engineToko
        engineToko()

        #Memanggin Fungsi engineBank

        #Memanggil Fungsi engineSingkronisasi
        engineSingkronisasi()

    except: # klo gagal tampilkan error
        print("ERROR!")
    time.sleep(10) # memberikan time delay selama 10 detik