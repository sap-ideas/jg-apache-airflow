# Knowledge: Komponen Comboset Identik ≠ Data Duplikat

Konteks: ETL transaksi iSeller (headers & details), notebook `transaction_janji_jiwa_pusat.ipynb`.
Dokumen ini hanya menjelaskan kasusnya. Tidak berisi solusi.
Solusi yang diimplementasikan di pipeline production ada di
[`FLOW_dedup_details_comboset.md`](FLOW_dedup_details_comboset.md) (folder yang sama).

---

## 1. Inti masalah

Satu bundling (comboset) bisa berisi **produk yang sama lebih dari sekali**, misalnya
"Thai Milk Tea Large Combo 3" (3 cup Thai Milk Tea) atau combo Americano ×5.

Setelah di-expand jadi row per komponen, row-row komponen itu **identik di semua kolom**
yang biasa dipakai sebagai composite key. Akibatnya proses delete duplicate menganggapnya
duplikat dan menyisakan **hanya 1 row**, padahal semua row itu sah dan mewakili qty yang nyata.

> Row yang terlihat kembar **belum tentu** duplikat. Di comboset, row kembar bisa berarti
> "ada N unit produk yang sama dalam satu paket".

---

## 2. Contoh raw data (API iSeller `GetOrders`)

Order `4233d232-b01d-4b8e-b37e-5b5aa878cfac` (`#460-21403`). Satu `order_detail` bertipe
comboset, dengan array `product_combos` berisi 3 entri yang **byte-identical**:

```json
{
  "order_detail_id": "8aa52f58-29b9-4928-b574-46f2c14f118c",
  "type": "comboset",
  "product_id": "405236b9-3ef6-4ec9-8490-07022257552e",
  "sku": "CS-2026-000491",
  "product_name": "Thai Milk Tea Large Combo 3",
  "quantity": 1.0,
  "total_order_amount": 92727.27,
  "product_combos": [
    {"product_id": "a38ab33c-3c64-6c04-18b9-d71c0775914f", "product_sku": "ITM022184", "product_name": "Thai Milk Tea", "variant_name": "Large/Ice", "quantity": 1.0},
    {"product_id": "a38ab33c-3c64-6c04-18b9-d71c0775914f", "product_sku": "ITM022184", "product_name": "Thai Milk Tea", "variant_name": "Large/Ice", "quantity": 1.0},
    {"product_id": "a38ab33c-3c64-6c04-18b9-d71c0775914f", "product_sku": "ITM022184", "product_name": "Thai Milk Tea", "variant_name": "Large/Ice", "quantity": 1.0}
  ]
}
```

Perhatikan:
- Di level `order_detail` hanya ada **1 row** (`quantity: 1.0` = 1 paket).
- Isi paketnya ada di `product_combos`: **3 entri**, tidak ada field apa pun yang membedakan
  satu entri dengan yang lain (tidak ada id komponen, tidak ada nomor urut).
- Jumlah entri di `product_combos` = jumlah unit komponen yang sebenarnya.

---

## 3. Bagaimana row kembar terbentuk di pipeline

1. `product_combos` di-normalize (`json_normalize(record_path='product_combos', meta='order_detail_id')`)
   → 3 row, semuanya ber-`order_detail_id` sama.
2. Di-merge ke details (`on='order_detail_id'`) → 1 `order_detail` comboset jadi 3 row.
3. Kolom `product_id`, `sku`, `product_name`, `quantity` diisi ulang dengan nilai komponen.
4. Hasil akhir di `df_final_details`:

| order_id | order_detail_id | type | bundling_id | product_id | sku | product_name |
|---|---|---|---|---|---|---|
| 4233d232… | 8aa52f58… | comboset | 405236b9… | a38ab33c… | ITM022184 | Thai Milk Tea |
| 4233d232… | 8aa52f58… | comboset | 405236b9… | a38ab33c… | ITM022184 | Thai Milk Tea |
| 4233d232… | 8aa52f58… | comboset | 405236b9… | a38ab33c… | ITM022184 | Thai Milk Tea |

Composite key yang dipakai saat ini:

```python
details_composite_key = [
    'order_id', 'order_detail_id', 'type', 'bundling_id', 'product_id', 'sku',
    'status', 'payment_status', 'transactions_type'
]
```

Ketiga row di atas **sama persis** untuk seluruh kolom ini → `drop_duplicates` menyisakan 1.

---

## 4. Dampak ke angka

Pembagian nilai combo ke tiap komponen terjadi **sebelum** delete duplicate.
Nilai paket dibagi rata ke N row (`group_size = N`), lalu N−1 row dibuang
→ **(N−1)/N nilai combo hilang** dari details.

Order `#460-21403`:

| | total_amount |
|---|---|
| Header (ground truth) | 43.740,00 |
| Details kalau 3 row komponen utuh | 43.740,00 (100,00%) |
| Details setelah delete duplicate (sisa 1 row Thai Milk Tea) | 29.927,37 (**68,42%**) |

Agregat data 11–17 Maret 2026 (4.056 order):

| Ukuran | Nilai |
|---|---|
| order_detail bertipe comboset | 715 |
| Yang punya komponen identik berulang | 52 (7,3%) |
| Row komponen sah yang ikut terhapus | 66 |
| Akurasi details/header kalau row terhapus | ~99,445% |
| Akurasi details/header kalau row utuh | ~99,812% |

Combo yang paling sering terdampak (struktur paketnya memang pasti berisi produk sama):
`B1G1 Coffee Series`, `Buy 1 Get 1 Minuman`, `Combo Toast 2 - Delivery`,
`Buy 2 Lite Toast Salted Egg Chicken`, `Jajan Berlima`, `QPON Coffee Break Combo`,
`B1G1 Flavored Tea Series`, `Tiga Toast Lebih Hemat`, `Special Hampers 2`.
Kasus serupa juga tercatat di study case: "Pear Shaken Americano" ×5 dalam satu paket.

---

## 5. Fakta pembeda: duplikat asli vs komponen sah

Fakta yang sudah diverifikasi dari data mentah — berguna sebagai bahan pertimbangan:

- **Data mentah API tidak mengandung duplikat.** Di `response.json` (327 order):
  327 `order_id` unik, 1.361 `order_detail_id` unik, 0 yang muncul lebih dari sekali.
- Artinya:
  - Row kembar yang **berasal dari `product_combos`** = komponen sah (jumlahnya ditentukan sumber).
  - Row kembar yang **muncul karena proses merge/join** di pipeline = duplikat buatan pipeline.
- Dari isi kolom saja, kedua jenis ini **tidak bisa dibedakan** — keduanya identik.
- `group_size` / `len(group)` bukan pembeda: nilainya sama di semua row dalam grup.
- Menghitung urutan row (`cumcount`) setelah proses merge juga bukan pembeda yang andal,
  karena jumlah row di titik itu sudah bisa ikut tergandakan oleh merge.
- Row comboset non-identik (komponen beda SKU) tidak terkena masalah ini, karena
  `product_id`/`sku`-nya sudah membedakan.

---

## 6. Kasus terkait: row kembar akibat transaksi `sale` + `refund`

Masih satu keluarga ("terlihat duplikat, padahal bukan"):

- Order yang di-refund punya **2 entri di array `transactions`** (`type: sale` dan `type: refund`),
  `order_id` sama, `transaction_id` beda.
- Refund **tidak** punya `order_detail` sendiri — jumlah `order_details` tetap = jumlah produk.
- Karena atribut header di-merge ke details `on='order_id'`, setiap row detail order itu
  jadi 2 row yang hanya beda di `transactions_type` (`sale` / `refund`).
- Contoh: `F/9455/9YCI.270726` (1 order_detail, 2 transaksi: sale 8.500 + refund 8.500),
  `DEL/AYU/SWEE.160326` (4 order_detail, 2 transaksi Jiwa+).
- Frekuensi kecil (≈0,05–0,6% order), tapi grain header dan details harus konsisten,
  kalau tidak rekonsiliasi jadi timpang.

---

## 7. Ringkasan untuk agent

- Comboset bisa sah berisi N row yang identik di semua kolom.
- Composite key berbasis `order_id, order_detail_id, type, bundling_id, product_id, sku, status,
  payment_status, transactions_type` **tidak cukup** untuk membedakan komponen identik.
- Menghapus row tersebut menghilangkan (N−1)/N nilai combo dan menurunkan akurasi details vs header.
- Sumber kebenaran jumlah komponen ada di array `product_combos` pada raw JSON.
  Raw JSON tidak tersimpan di Data Lake, jadi pipeline dedup memakai pembanding lain yang tersedia: jumlah slot
  bundling dari master API `GetProducts` (lihat FLOW).
- Delete duplicate harus hanya membuang row yang benar-benar duplikat, bukan komponen combo yang sah.
