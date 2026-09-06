# Ansible deployment

Folder ini dipakai untuk menyiapkan cluster penelitian Spark–Kafka pada tujuh VPS Ubuntu 24.04.
Playbook menginstal software dan menulis konfigurasi service. Pembuatan VPS, private network, volume,
dan security group tetap dilakukan di panel atau API provider.

## Topologi

| Host group | Jumlah | Service |
|---|---:|---|
| `kafka` | 3 | Kafka broker sekaligus KRaft controller |
| `spark_master` | 1 | Spark standalone master |
| `spark_workers` | 2 | Spark standalone worker |
| `controller` | 1 | Repository, capture/replay, Prometheus, Grafana, Kafka Exporter |

Semua VPS cluster dalam satu run harus memakai profile yang sama. Controller tidak termasuk node
yang dibandingkan.

| Profile | RAM/VPS | vCPU sementara | Disk sementara |
|---|---:|---:|---:|
| `low` | 4 GB | 2 | 50 GB |
| `medium` | 8 GB | 4 | 100 GB |
| `high` | 32 GB | 8 | 200 GB |

Angka vCPU dan disk masih nilai awal. Kunci tipe instance dan region sebelum mengambil data final.

## Isi folder

```text
deploy/ansible/
├── group_vars/all.yml
├── inventory.example.yml
├── requirements.yml
├── site.yml
└── playbooks/
    ├── preflight.yml
    ├── common.yml
    ├── kafka.yml
    ├── spark.yml
    ├── controller.yml
    ├── stream-config.yml
    ├── capture.yml
    └── verify.yml
```

`site.yml` menjalankan semua tahap instalasi kecuali capture. `capture.yml` sengaja dipisahkan karena
ia menghubungi sumber data publik dan dapat berjalan sampai 15 menit.

## Prasyarat

Sebelum menjalankan playbook, pastikan:

- tujuh VPS sudah aktif dan berada dalam satu private network;
- seluruh host memakai Ubuntu 24.04;
- controller Ansible dapat SSH ke semua host menggunakan key;
- private IP tidak berubah selama eksperimen;
- waktu sistem disinkronkan;
- repository sudah tersedia melalui Git URL yang dapat diakses controller;
- Ansible tersedia pada komputer yang menjalankan deployment.

Instalasi Ansible pada komputer operator dapat menggunakan package manager sistem atau environment
Python terpisah. Periksa dengan:

```bash
ansible-playbook --version
```

## Menyiapkan inventory

Salin contoh inventory. File aktual tidak dilacak Git.

```bash
cd deploy/ansible
cp inventory.example.yml inventory.yml
```

Edit `inventory.yml`:

```yaml
all:
  vars:
    ansible_user: ubuntu
    ansible_ssh_private_key_file: /absolute/path/to/private-key
```

Ganti `ansible_host` dan `private_ip` untuk ketujuh host. `ansible_host` adalah alamat yang dapat
dijangkau komputer operator. `private_ip` adalah alamat yang dipakai komunikasi Kafka, Spark, dan
monitoring. Keduanya boleh sama apabila Ansible dijalankan dari private network.

Uji akses sebelum instalasi:

```bash
ansible -i inventory.yml all -m ping
```

Jika langkah ini gagal, jangan lanjut ke playbook. Periksa SSH user, key, firewall provider, dan
alamat host terlebih dahulu.

## Mengatur variabel

Edit `group_vars/all.yml`. Variabel yang wajib diperiksa:

```yaml
deployment_profile: low
private_network_cidr: 10.0.0.0/24
admin_cidr: 203.0.113.10/32
project_repository_url: https://git.example.org/team/project.git
project_version: main
wikimedia_user_agent: SparkKafkaResearch/1.0 (nama@example.com)
```

Gunakan CIDR publik administrator yang sebenarnya untuk `admin_cidr`. Preflight akan menolak nilai
contoh. Jangan menaruh private key, token Git, password Grafana, atau credential provider di file
ini. Untuk repository privat, gunakan deploy key atau credential store di luar repository.

Pilih satu profile untuk satu deployment:

```yaml
deployment_profile: low
```

Setelah seluruh run Low selesai dan artefaknya telah disalin, cluster dapat di-resize atau dibuat
ulang sebagai Medium, kemudian High. Catat tipe instance aktual pada manifest eksperimen.

## Instalasi

Pasang collection yang digunakan oleh playbook:

```bash
ansible-galaxy collection install -r requirements.yml
```

Periksa sintaks dan perubahan yang akan dilakukan:

```bash
ansible-playbook -i inventory.yml site.yml --syntax-check
ansible-playbook -i inventory.yml site.yml --check
```

Mode `--check` tidak dapat mensimulasikan semua command systemd, download, atau Docker dengan
sempurna. Gunakan hasilnya sebagai pemeriksaan awal, bukan bukti deployment berhasil.

Jalankan instalasi:

```bash
ansible-playbook -i inventory.yml site.yml
```

Urutan yang dijalankan:

1. memvalidasi OS, inventory, profile, dan jumlah host;
2. memasang Java, Chrony, UFW, dan Node Exporter;
3. memasang Kafka 4.1.1 pada tiga broker;
4. membuat topic Wikimedia dan Binance;
5. memasang Spark 4.0.1 pada master dan workers;
6. memasang repository serta `uv` pada controller;
7. menjalankan Prometheus, Grafana, dan Kafka Exporter;
8. menulis konfigurasi stream dan menghasilkan 54 scenario;
9. memeriksa endpoint dasar cluster.

Playbook bersifat idempotent untuk sebagian besar instalasi. Task pembaruan repository, sinkronisasi
dependency, pembuatan scenario, dan Docker Compose tetap dapat dilaporkan sebagai berubah ketika
playbook dijalankan ulang.

## Konfigurasi yang dihasilkan

Pada controller:

```text
/etc/spark-kafka-research/experiment.yml
/etc/spark-kafka-research/runtime.env
/opt/spark-kafka-research/config/cloud-generated/run-plan.csv
```

`experiment.yml` memuat broker Kafka, Spark master, path capture, path hasil, profile aktif, serta
load 1.000, 5.000, dan 10.000 event/detik. `runtime.env` dibuat dengan mode `0640` karena dapat
mengandung informasi jaringan internal.

Pada node Kafka:

```text
/etc/kafka/server.properties
/etc/systemd/system/kafka.service
/var/lib/kafka/data
```

Pada node Spark:

```text
/opt/spark-4.0.1-bin-hadoop3/conf/spark-defaults.conf
/etc/systemd/system/spark-master.service
/etc/systemd/system/spark-worker.service
```

## Capture data

Isi `wikimedia_user_agent` dengan identitas dan kontak yang benar, lalu jalankan:

```bash
ansible-playbook -i inventory.yml playbooks/capture.yml
```

Playbook mengambil:

- Wikimedia `recentchange` melalui SSE;
- Binance Spot `aggTrade` untuk BTCUSDT, ETHUSDT, dan BNBUSDT melalui WebSocket.

Hasil disimpan di `capture_path`, default `/srv/bigdata-captures`, sebagai `events.ndjson.gz` dan
`manifest.json`. Capture tidak idempotent: setiap eksekusi membuat capture baru. Simpan capture yang
dipilih dan gunakan artefak identik untuk semua profile.

## Skenario eksperimen

Matriks yang dihasilkan berisi:

```text
3 profile VPS × 3 tingkat load × 2 dataset × 3 repetisi = 54 run
```

| Load | Target |
|---|---:|
| Low | 1.000 event/detik |
| Medium | 5.000 event/detik |
| High | 10.000 event/detik |

Urutan run sudah diacak secara deterministik dalam `run-plan.csv`. Target rate adalah offered load;
throughput yang benar-benar dicapai tetap dicatat sebagai hasil.

Instalasi ini belum otomatis menjalankan seluruh 54 run. Setiap run tetap harus melalui prepare,
Spark submit, replay, pengambilan metrik, query benchmark, finalisasi, dan cooldown. Pemisahan ini
mencegah satu perintah setup langsung menjalankan eksperimen berjam-jam tanpa pemeriksaan operator.

## Monitoring

Grafana berjalan pada controller port `3000` dan Prometheus pada `9090`. Keduanya tidak perlu dibuka
ke publik. Gunakan SSH tunnel:

```bash
ssh -L 3000:127.0.0.1:3000 ubuntu@<CONTROLLER_PUBLIC_IP>
```

Lalu buka `http://127.0.0.1:3000`.

Prometheus mengambil Node Exporter dari enam node cluster, Kafka Exporter, dan metric aplikasi Spark.
Gunakan label host/role saat menganalisis CPU, RAM, disk, dan network agar Kafka tidak tercampur
dengan Spark.

## Pemeriksaan manual

Setelah deployment, jalankan ulang verifikasi:

```bash
ansible-playbook -i inventory.yml playbooks/verify.yml
```

Cek service jika verifikasi gagal:

```bash
ansible -i inventory.yml kafka -b -a 'systemctl status kafka --no-pager'
ansible -i inventory.yml spark_master -b -a 'systemctl status spark-master --no-pager'
ansible -i inventory.yml spark_workers -b -a 'systemctl status spark-worker --no-pager'
ansible -i inventory.yml all -b -a 'systemctl status node-exporter --no-pager'
```

Log dapat dibaca dengan:

```bash
ansible -i inventory.yml kafka -b -a 'journalctl -u kafka -n 100 --no-pager'
ansible -i inventory.yml spark_workers -b -a 'journalctl -u spark-worker -n 100 --no-pager'
```

## Menjalankan ulang dengan aman

Sebelum rerun, lihat perubahannya:

```bash
ansible-playbook -i inventory.yml site.yml --check --diff
```

Jangan menghapus `/var/lib/kafka/data`, checkpoint, capture, atau hasil eksperimen hanya untuk
memperbaiki service. Data tersebut dihapus hanya ketika run dan backup yang terkait sudah dipastikan.

## Batasan saat ini

- Playbook belum membuat VPS, VPC, volume, DNS, atau security group provider.
- Kafka memakai PLAINTEXT di private network. TLS/SASL belum dikonfigurasi.
- Shared/object storage untuk hasil Spark belum diprovision otomatis.
- Grafana dibuat tanpa anonymous access, tetapi credential admin tetap harus dikelola operator.
- Checksum artefak Kafka, Spark, dan exporter belum dipin di variables.
- Orkestrasi otomatis 54 run dan fault injection belum tersedia.

Jangan menganggap deployment final aman untuk jaringan publik sebelum TLS, autentikasi, secret
management, checksum artefak, dan aturan firewall provider selesai.
