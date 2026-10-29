================================================================================
                    LAPORAN QUALITY CHECK & RECONCILIATION
                           GOLD LAYER DATA PIPELINE
================================================================================
Tanggal Pemeriksaan : 29 Oktober 2026
Pipeline ID        : omnichannel_retail_pipeline
Run ID Terakhir    : manual__2026-10-29T15:29:09.774232+00:00
Status Akhir       : SUCCESS / PASS

--------------------------------------------------------------------------------
1. RINGKASAN REKONSILIASI ULANG & STATUS LAYERING (SUMMARY)
--------------------------------------------------------------------------------
+-----------------------------+--------------+--------------+
| Nama Tabel Gold             | Jumlah Baris | Status       |
+-----------------------------+--------------+--------------+
| gold.order_360              |       10,000 | PASS         |
| gold.customer_daily         |       12,520 | PASS         |
| gold.product_daily          |        7,201 | PASS         |
| gold.channel_campaign_daily |        8,834 | PASS         |
| gold.executive_kpis_daily   |           90 | PASS         |
+-----------------------------+--------------+--------------+
STATUS GOLD COUNT: GOLD COUNT PASS

--------------------------------------------------------------------------------
2. DETIL ANOMALI & UJI KUALITAS DATA (QUALITY GATES)
--------------------------------------------------------------------------------

[A] REKONSILIASI BARIS (SILVER ELIGIBLE VS GOLD)
- Silver Eligible Orders : 10,000
- Gold Order 360         : 10,000
- Selisih (Difference)   : 0
- Status                 : PASS

[B] REKONSILIASI REVENUE (FINANCIAL ACCURACY)
- Total Valid Rows       : 10,000
- Total Invalid Rows     : 0
- Sample Fail Revenue    : 0 rows (Tidak ditemukan kesalahan selisih revenue)
- Status                 : PASS

[C] CHECK NULL CRITICAL COLUMNS
- Null Order ID          : 0
- Null Customer ID       : 0
- Null Order Date        : 0
- Null Net Revenue       : 0
- Status                 : PASS

[D] LOGIKA DISKON (ITEM + ORDER PROMO RECONCILIATION)
- Formula                : gold_disc = silver_item_disc + silver_promo_disc
- Pengujian Sample       : 5 sample diuji (ORD-000001 s/d ORD-000005)
- Hasil Contoh           :
    * ORD-000003: 5.00 (Item) + 10.00 (Promo) = 15.00 (Gold Disc) [PASS]
    * ORD-000004: 5.00 (Item) + 30.00 (Promo) = 35.00 (Gold Disc) [PASS]
- Status                 : PASS

[E] PENGUJIAN INTEGRITAS TABEL CHANNEL_CAMPAIGN_DAILY
- Total Baris            : 8,834
- Kolom Refunds Null     : 0
- Status Null Refunds    : PASS

--------------------------------------------------------------------------------
3. LOG EKSEKUSI PIPELINE & AIRFLOW SCHEDULER
--------------------------------------------------------------------------------

[A] OPS LOG PIPELINE RUNS
- Status Eksekusi        : SUCCESS
- Durasi Run Terakhir    : 00:00:03.317706
- Quality Gate Checks    :
    * gold_reconciliation           : PASS
    * SILVER_TO_GOLD_QUALITY_GATE  : PASS (null_cnt: 0, dup_cnt: 0, 
                                            financial_diff_cnt: 0)

[B] AIRFLOW TASK EXECUTION LOG (build_gold_layer)
- Task ID                : build_gold_layer
- Execution Date         : 2026-10-29T15:29:09
- Completion Log         :
    * [VALIDATE] gold=10000 eligible=10000 invalid=0
    * Gold refresh complete: SUCCESS
    * Task marked as SUCCESS (Return code 0)

[C] WARNINGS / MINOR ISSUES FOR MAINTENANCE (NON-BLOCKING)
1. OpenLineage Event Warning:
   "Failed to emit OpenLineage event of id..." -> Transport HTTP OpenLineage 
   mengalami masalah koneksi/endpoint. Tidak mempengaruhi integritas data Gold.
2. Adapter Type Warning:
   "[OPS LOG] Warning tracking pipeline run: can't adapt type 'DAG'" -> Terdapat 
   pasing tipe data objek DAG ke tracker log Ops. Data tetap sukses terproses.

--------------------------------------------------------------------------------
4. KESIMPULAN
--------------------------------------------------------------------------------
Seluruh kriteria Quality Check dari Silver ke Gold Layer telah terpenuhi 100%. 
Tidak ada kebocoran data, perbedaan nilai finansial/revenue, maupun nilai NULL 
pada kolom kritikal. Pipeline berjalan aman dan siap digunakan untuk kebutuhan 
pelaporan/executive dashboard.
================================================================================
