"""Transform functions — payload per order + guard anti-dobel (cek ke api_landlord_log_detail)."""

from datetime import datetime

import pandas as pd

MAX_TN_LEN = 50   # mall memotong TransactionNumber > 50 char (batch 1553125, 2026-11-20) -> respons gak cocok lagi dgn log

PAYLOAD_COLUMNS = [
    'jilid', 'TransactionDate', 'PaymentType', 'TotalSales', 'TotalTax', 'Amount',
    'TransactionNumber', 'Remarks', '_key',
]


def build_payload(df_order: pd.DataFrame, run_timestamp: datetime) -> tuple[pd.DataFrame, int]:
    """Satu baris payload per order + payment type.

    TransactionNumber = <jilid>_<order_reference>_<PaymentType>_<HHMMSS>   (HHMMSS = jam run WIB, buat revisi)
    Remarks           = <jilid>_<order_reference>_<PaymentType>_<YYYY-MM-DD>_<HHMMSS>
    Amount            = ROUND(subtotal - sales_type_commission, 4)       (uang masuk penuh; 10% dihitung landlord)
    _key              = TransactionNumber tanpa _HHMMSS (kunci guard)
    Baris total_sales = 0 tidak dikirim.

    Raises:
        ValueError: TransactionNumber dobel (split payment dgn payment type sama) atau > MAX_TN_LEN char.

    Returns:
        (final_data, jumlah baris total_sales = 0 yang di-skip)
    """
    if df_order.empty:
        return pd.DataFrame(columns=PAYLOAD_COLUMNS), 0

    est = df_order.copy()
    for col in ['subtotal', 'tax_amount', 'total_sales', 'sales_type_commission']:
        est[col] = pd.to_numeric(est[col]).astype(float)
    est['date'] = est['date'].astype(str)

    n_zero = int((est['total_sales'] == 0).sum())
    est = est[est['total_sales'] != 0].reset_index(drop=True)
    if est.empty:
        return pd.DataFrame(columns=PAYLOAD_COLUMNS), n_zero

    ts_str = run_timestamp.strftime("%H%M%S")
    base = (est['outlet_code'].astype(str) + '_' + est['order_reference'].astype(str)
            + '_' + est['payment_type'].astype(str))

    final_data = pd.DataFrame({
        'jilid':           est['outlet_code'].astype(str),
        'TransactionDate': est['date'],
        'PaymentType':     est['payment_type'],
        'TotalSales':      est['total_sales'],
        'TotalTax':        est['tax_amount'],
        # dibulatkan 4 desimal hanya di output yang dikirim
        'Amount':          (est['subtotal'] - est['sales_type_commission']).round(4),
        'TransactionNumber': base + '_' + ts_str,
        'Remarks':         base + '_' + est['date'] + '_' + ts_str,
        '_key':            base,
    })

    dup = final_data[final_data['TransactionNumber'].duplicated(keep=False)]
    if not dup.empty:
        raise ValueError(
            f"{len(dup)} TransactionNumber dobel (1 order dibayar 2x dgn payment type sama?), cek sumber dulu:\n"
            + dup[['TransactionDate', 'PaymentType', 'TransactionNumber']].to_string(index=False)
        )
    too_long = final_data[final_data['TransactionNumber'].str.len() > MAX_TN_LEN]
    if not too_long.empty:
        raise ValueError(
            f"{len(too_long)} TransactionNumber > {MAX_TN_LEN} char (mall memotongnya). Pendekkan format dulu:\n"
            + too_long[['TransactionDate', 'PaymentType', 'TransactionNumber']].to_string(index=False)
        )

    return final_data[PAYLOAD_COLUMNS], n_zero


def apply_guard(final_data: pd.DataFrame, log_all: pd.DataFrame, jilid: str,
                resend_uncertain: bool, dry_run: bool) -> tuple[pd.DataFrame, dict]:
    """Buang baris yang tidak boleh dikirim. Kunci = TransactionNumber tanpa _HHMMSS.

    | Status di log untuk kunci ini                  | Aksi                                              |
    |-----------------------------------------------|---------------------------------------------------|
    | success / error + TRIM(error_message)='duplicate' | skip (sudah ada di mall)                      |
    | timeout_error / pending (tanpa success/duplicate) | skip + peringatan, kecuali resend_uncertain   |
    | http_error / error lain                        | kirim ulang (mall menolak, belum masuk)           |
    | tidak ada di log                               | kirim                                             |

    Tanggal yang sudah diterima mall dalam format lama (per payment type) -> raise (kecuali dry_run).

    Returns:
        (to_send, info) info: already, uncertain (DataFrame), dup_only (DataFrame), legacy (DataFrame)
    """
    log = log_all.copy()
    log['transaction_number'] = log['transaction_number'].astype(str)
    # baris log per order = diawali '<jilid>_' dan diakhiri _HHMMSS; selain itu (Janji_Jiwa_...) = format lama per payment type
    is_per_order = (log['transaction_number'].str.startswith(jilid + '_')
                    & log['transaction_number'].str.contains(r'_\d{6}$'))
    # buang _HHMMSS, lalu _YYYYMMDD kalau ada (format test 2026-11-20 masih pakai tanggal)
    log['key'] = (log['transaction_number'].str.replace(r'_\d{6}$', '', regex=True)
                  .str.replace(r'_\d{8}$', '', regex=True).where(is_per_order))
    log['transaction_date'] = log['transaction_date'].astype(str)
    is_success   = log['status'] == 'success'
    is_duplicate = log['error_message'].fillna('').astype(str).str.strip().eq('duplicate')
    is_uncertain = log['status'].isin(['timeout_error', 'pending'])
    log['accepted'] = is_success | is_duplicate
    has_key = log['key'].notna()

    # 1) tanggal yang sudah diterima mall dalam format lama -> STOP (kirim lagi per order = dobel)
    legacy = log[~has_key & log['accepted'] & log['transaction_date'].isin(set(final_data['TransactionDate']))]
    if not legacy.empty:
        summary = legacy.groupby('transaction_date')['amount'].agg(['count', 'sum']).to_string()
        msg = ("Range berisi tanggal yang sudah diterima mall dalam format lama (per payment type), "
               f"kirim lagi per order = dobel:\n{summary}")
        if not dry_run:
            raise ValueError(msg)
        print(f"        PERINGATAN (dry run): {msg}")

    # 2) sudah pasti di mall -> skip
    accepted_keys = set(log.loc[log['accepted'] & has_key, 'key'])
    already = final_data['_key'].isin(accepted_keys)

    # 3) timeout / pending & belum pernah pasti diterima -> belum tentu sampai mall -> skip + peringatan
    uncertain_keys = set(log.loc[is_uncertain & has_key, 'key']) - accepted_keys
    uncertain = final_data['_key'].isin(uncertain_keys)

    # 4) 'duplicate' tanpa pernah 'success' di log kita -> info
    success_keys = set(log.loc[is_success & has_key, 'key'])
    dup_only = log[is_duplicate & has_key & ~log['key'].isin(success_keys)]

    skip = already | (uncertain & (not resend_uncertain))
    to_send = final_data[~skip].reset_index(drop=True)
    info = {
        'already':   int(already.sum()),
        'uncertain': final_data[uncertain].reset_index(drop=True),
        'dup_only':  dup_only.reset_index(drop=True),
        'legacy':    legacy.reset_index(drop=True),
    }
    return to_send, info
