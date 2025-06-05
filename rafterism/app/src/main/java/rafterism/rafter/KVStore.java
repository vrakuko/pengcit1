package rafterism.rafter;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set; // Import Set untuk method keys()


public class KVStore {
    // Menggunakan ConcurrentHashMap untuk memastikan thread-safety
    // jika ada skenario di mana KVStore bisa diakses langsung oleh beberapa thread
    // (meskipun dalam konteks Raft, KVStore harusnya diakses secara single-threaded
    // oleh RaftNode setelah log di-commit).
    // Namun, HashMap biasa juga cukup jika akses selalu diatur oleh RaftNode.
    private Map<String, String> kvStore;

    public KVStore() {
        this.kvStore = new HashMap<>(); // Tetap pakai HashMap, karena akses diatur oleh RaftNode
    }

    /**
     * Mengembalikan nilai dari key yang diberikan.
     * Mengembalikan string kosong jika key belum ada.
     * @param key Kunci yang dicari.
     * @return Nilai yang terkait dengan kunci, atau string kosong jika tidak ada.
     */
    public String get(String key) {
        return kvStore.getOrDefault(key, "");
    }

    /**
     * Menetapkan nilai untuk key yang diberikan.
     * Jika key sudah ada, akan menimpa nilai lama.
     * Ini adalah operasi write (perubahan state) yang harus melalui Raft log.
     * @param key Kunci yang akan diset.
     * @param value Nilai yang akan diset.
     */
    public void set(String key, String value) {
        kvStore.put(key, value);
        // Dalam implementasi Raft, System.out.println("OK") atau respons lain
        // umumnya dilakukan oleh RaftNode setelah command berhasil di-commit dan di-apply.
        // Di sini, KVStore hanya mengubah state.
    }

    /**
     * Menghapus entry dari key yang diberikan.
     * @param key Kunci yang akan dihapus.
     * @return Nilai yang dihapus, atau string kosong jika key tidak ada.
     * Ini adalah operasi write (perubahan state) yang harus melalui Raft log.
     */
    public String del(String key) {
        // Menggunakan remove(key) akan mengembalikan nilai yang dihapus
        String value = kvStore.remove(key);
        return value != null ? value : ""; // Mengembalikan string kosong jika tidak ada
    }

    /**
     * Menambahkan (append) nilai ke nilai yang sudah ada untuk key yang diberikan.
     * Jika key belum ada, key akan dibuat dengan nilai string kosong sebelum append.
     * Ini adalah operasi write (perubahan state) yang harus melalui Raft log.
     * @param key Kunci yang akan di-append.
     * @param value Nilai yang akan ditambahkan.
     */
    public void append(String key, String value) {
        String current = kvStore.getOrDefault(key, ""); // Ambil nilai saat ini, atau "" jika tidak ada
        kvStore.put(key, current + value);
        // Sama seperti set, respons dilakukan di RaftNode.
    }

    /**
     * Mengembalikan panjang (length) dari nilai yang terkait dengan key yang diberikan.
     * @param key Kunci yang dicari panjang nilainya.
     * @return Panjang nilai, atau 0 jika key tidak ada.
     */
    public int strLen(String key) {
        return get(key).length(); // Menggunakan get() untuk menangani kasus key tidak ada (mengembalikan "").
    }

    /**
     * Fungsi ping untuk mengecek koneksi/ketersediaan.
     * Ini adalah operasi read-only yang tidak mengubah state,
     * jadi tidak perlu melalui Raft log.
     * @return String "PONG".
     */
    public String ping() {
        return "PONG";
    }

    // --- Penambahan Fungsi yang Mungkin Berguna ---

    /**
     * Mengambil semua kunci yang ada di KVStore.
     * Berguna untuk debugging atau inspeksi state.
     * @return Set dari semua kunci.
     */
    public Set<String> keys() {
        return Collections.unmodifiableSet(kvStore.keySet());
    }

    /**
     * Mendapatkan snapshot dari seluruh KVStore.
     * Berguna untuk log compaction atau transfer state awal ke node baru.
     * @return Sebuah Map baru yang berisi salinan dari KVStore.
     */
    public Map<String, String> getSnapshot() {
        // Membuat salinan untuk menghindari modifikasi eksternal
        return new HashMap<>(kvStore);
    }

    /**
     * Memulihkan state KVStore dari snapshot.
     * Berguna saat bootstrapping node baru atau setelah log compaction.
     * @param snapshot Map yang berisi state yang akan dipulihkan.
     */
    public void restoreSnapshot(Map<String, String> snapshot) {
        this.kvStore.clear(); // Hapus state lama
        this.kvStore.putAll(snapshot); // Masukkan state baru
        System.out.println("KVStore restored from snapshot.");
    }

    /**
     * Mengosongkan seluruh KVStore.
     * Berguna untuk pengujian atau reset.
     */
    public void clear() {
        kvStore.clear();
        System.out.println("KVStore cleared.");
    }

    /**
     * Untuk representasi string dari KVStore (debugging).
     */
    @Override
    public String toString() {
        return "KVStore{" +
               "size=" + kvStore.size() +
               ", data=" + kvStore +
               '}';
    }
}
