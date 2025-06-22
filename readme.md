# Tugas Besar IF3130 Sistem Paralel dan terdistribusi 

## Implementasi Algoritma Konsensus Raft Sederhana

### Kelompok Sisterism

Proyek ini adalah implementasi dari protokol konsensus Raft sederhana yang dibangun di atas sebuah *distributed key-value in-memory store*. Proyek ini dikembangkan sebagai bagian dari tugas besar untuk mata kuliah IF3130 Sistem Paralel dan Terdistribusi.

Sistem ini dirancang untuk dijalankan sebagai sebuah cluster yang terdistribusi dalam jaringan virtual menggunakan Docker, sesuai dengan spesifikasi tugas.

## Fitur Utama

### Protokol Raft
-   **Leader Election:** Mekanisme pemilihan Leader yang tangguh, termasuk saat terjadi kegagalan node.
-   **Log Replication:** Sinkronisasi log perintah dari Leader ke semua Follower untuk menjamin konsistensi state.
-   **Heartbeats:** Leader secara periodik mengirimkan heartbeat untuk mempertahankan kepemimpinannya dan memberitahu Follower bahwa ia masih aktif.
-   **Failover Handling:** Cluster dapat secara otomatis memilih Leader baru jika Leader saat ini mati atau tidak dapat dijangkau.

### Aplikasi Key-Value Store
-   Menyediakan perintah `set`, `get`, `append`, `del`, `strln`, dan `ping`.
-   Semua operasi yang mengubah state dijamin konsisten di seluruh node dalam cluster.

### Arsitektur
-   **Komunikasi Antar-Node:** Menggunakan **gRPC** untuk komunikasi RPC yang efisien.
-   **API Klien:** Setiap node mengekspos API HTTP sederhana menggunakan **SparkJava** untuk interaksi dengan klien.
-   **Containerization:** Seluruh cluster di-deploy menggunakan **Docker**, yang memungkinkan simulasi jaringan terdistribusi di satu mesin.

## Teknologi yang Digunakan

-   **Bahasa:** Java 11
-   **Build Tool:** Gradle
-   **RPC Framework:** gRPC & Protocol Buffers
-   **Web Framework (API Klien):** SparkJava
-   **Containerization:** Docker

## Cara Menjalankan Program

Proyek ini dirancang untuk dijalankan sebagai cluster terdistribusi menggunakan Docker, sesuai dengan spesifikasi tugas yang memerlukan penggunaan jaringan virtual.

### Prasyarat

1.  **Java Development Kit (JDK) 17** atau yang lebih baru.
2.  **Docker Desktop** terinstal dan sedang berjalan.
3.  **Git** untuk meng-clone repository.

### Langkah 1: Build Aplikasi dan Docker Image

Pertama, kita perlu mengkompilasi aplikasi Java dan membungkusnya ke dalam sebuah Docker image.

1.  Buka terminal dan navigasi ke direktori root proyek (`rafterism/`).
2.  Jalankan perintah Gradle untuk membuat distribusi aplikasi:
    ```bash
    ./gradlew installDist
    ```
3.  Setelah selesai, bangun Docker image menggunakan `Dockerfile` yang telah disediakan:
    ```bash
    docker build -t raft-app .
    ```

### Langkah 2: Siapkan Jaringan Virtual Docker

Agar semua node (container) dapat berkomunikasi satu sama lain, kita perlu membuat sebuah jaringan virtual khusus.

```bash
docker network create raft-net
```

### Langkah 3: Jalankan Cluster Raft

Kita akan menjalankan cluster yang terdiri dari 4 node. Setiap node akan berjalan sebagai container Docker yang terpisah. Buka satu terminal dan jalankan ketiga perintah berikut secara berurutan.

*   **Jalankan Node 1:**
    ```bash
    docker run -d --rm --network raft-net --name raft-node-1 -p 9001:9001 raft-app 0 raft-node-1:8001,raft-node-2:8002,raft-node-3:8003,raft-node-4:8004
    ```
*   **Jalankan Node 2:**
    ```bash
    docker run -d --rm --network raft-net --name raft-node-2 -p 9002:9002 raft-app 1 raft-node-1:8001,raft-node-2:8002,raft-node-3:8003,raft-node-4:8004
    ```
*   **Jalankan Node 3:**
    ```bash
    docker run -d --rm --network raft-net --name raft-node-3 -p 9003:9003 raft-app 2 raft-node-1:8001,raft-node-2:8002,raft-node-3:8003,raft-node-4:8004
    ```
*   **Jalankan Node 4:**
    ```bash
    docker run -d --rm --network raft-net --name raft-node-4 -p 9004:9004 raft-app 2 raft-node-1:8001,raft-node-2:8002,raft-node-3:8003,raft-node-4:8004
    ```


Untuk memeriksa apakah semua container berjalan dengan benar, gunakan perintah:
```bash
docker ps
```
Anda seharusnya melihat `raft-node-1`, `raft-node-2`, `raft-node-3`, dan `raft-node-4` dalam daftar.

### Langkah 4: Amati Proses Leader Election

Anda dapat melihat log dari setiap node untuk mengamati proses pemilihan leader dan pertukaran heartbeat. Buka terminal baru untuk setiap perintah log.

*   **Lihat Log Node 1:**
    ```bash
    docker logs -f raft-node-1
    ```
*   **Lihat Log Node 2:**
    ```bash
    docker logs -f raft-node-2
    ```
*   **Lihat Log Node 3:**
    ```bash
    docker logs -f raft-node-3
    ```
*   **Lihat Log Node 4:**
    ```bash
    docker logs -f raft-node-4
    ```

Tunggu beberapa saat, dan Anda akan melihat salah satu node mencetak log bahwa ia telah menjadi `LEADER`.

### Langkah 5: Jalankan Klien CLI

Setelah cluster stabil dan memiliki Leader, Anda dapat berinteraksi dengannya menggunakan Klien Command-Line (CLI).

1.  Buka terminal baru di komputer Anda (di luar Docker).
2.  Pastikan Anda berada di direktori root proyek (`rafterism/`).
3.  Jalankan perintah berikut untuk memulai klien:

    *   **Untuk macOS/Linux:**
        ```bash
        java -cp "app/build/classes/java/main:app/build/resources/main:app/build/install/app/lib/*" raft.RaftCLI localhost:9001,localhost:9002,localhost:9003,localhost:9004
        ```
    *   **Untuk Windows:**
        ```powershell
        java -cp "app/build/classes/java/main;app/build/resources/main;app/build/install/app/lib/*" raft.RaftCLI localhost:9001,localhost:9002,localhost:9003,localhost:9004
        ```
4.  Klien sekarang siap menerima perintah seperti `set`, `get`, `append`, `del`, `strln`, dan `ping`.

### Langkah 6: Hentikan Cluster

Jika sudah selesai, Anda dapat menghentikan dan menghapus semua container dengan perintah berikut:
```bash
docker stop raft-node-1 raft-node-2 raft-node-3 raft-node-4
```
Karena kita menggunakan flag `--rm` saat menjalankan, container akan otomatis terhapus setelah dihentikan. Anda juga bisa menghapus jaringan virtual jika tidak lagi dibutuhkan:
```bash
docker network rm raft-net
```

## Kontributor
| Nama.             | NIM              |
| ----------------- | ---------------- |
| Varraz Hazzandra Abrar      | 13521020       |
| Kharris Khisunica    | 13522051       |
| Fabian Radenta Bangun  | 13522105  |