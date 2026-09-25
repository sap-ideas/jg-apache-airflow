"""Transform functions - build the hourly rider performance final DataFrame.

Three data-quality cases are handled before the final merge:

  Case 1 — Transaction hour is OUTSIDE the rider's TI-TO working hour range
            for that day (e.g. transaction at 21:00 but shift ends at 19:00).
            Resolution: snap the transaction's hour to the nearest boundary
            (slot_min / slot_max), then re-aggregate to avoid double-counting.

  Case 2 — Transaction exists but the rider has NO TI-TO slot at all for
            that date. Resolution: synthesise hour-slots from the min/max
            transaction hour so the rider still appears in the output.

  Case 3 — Rider has TI-TO slots but ZERO transactions for the entire day.
            Resolution: exclude that rider+date combination entirely
            (no data = no row).
"""

import numpy as np
import pandas as pd


def build_hourly_performance(
    df_hourly_transactions,
    df_rider_timeslot,
    df_rider_mapping,
    load_data_by,
):
    """Merge raw extracts into the final hourly rider performance DataFrame.

    Args:
        df_hourly_transactions: output of extract.get_rider_hourly_transactions
        df_rider_timeslot:      output of extract.get_rider_timeslot_tito
        df_rider_mapping:       output of extract.get_rider_mapping
        load_data_by:           string tag recorded in the load_data_by column

    Returns:
        DataFrame ready to be loaded into hourly_sejutajiwa_rider_performance.
    """
    # ── Step 1: Filter to HUB riders only (SJO riders don't do TI-TO) ──────
    list_rider_hub = (
        df_rider_mapping[df_rider_mapping['sejuta_jiwa_type'] == 'HUB']['rider_id']
        .unique()
        .tolist()
    )
    df_trx = df_hourly_transactions[
        df_hourly_transactions['rider_code'].isin(list_rider_hub)
    ].copy()

    # ── Step 2: Normalise dtypes ──────────────────────────────────────────────
    df_trx['date'] = pd.to_datetime(df_trx['date'])
    df_trx['hour'] = df_trx['hour'].astype(int)

    df_timeslot = df_rider_timeslot.copy()
    df_timeslot['date'] = pd.to_datetime(df_timeslot['date'])
    df_timeslot['hour_slot'] = df_timeslot['hour_slot'].astype(int)

    # ── Step 3a: Case 1 — snap out-of-range transaction hours ────────────────
    df_slot_range = (
        df_timeslot
        .groupby(['rider_code', 'date'])
        .agg(slot_min=('hour_slot', 'min'), slot_max=('hour_slot', 'max'))
        .reset_index()
    )

    df_trx = df_trx.merge(df_slot_range, on=['rider_code', 'date'], how='left')

    df_trx['hour'] = np.where(
        df_trx['slot_min'].notna() & (df_trx['hour'] < df_trx['slot_min']),
        df_trx['slot_min'],
        np.where(
            df_trx['slot_max'].notna() & (df_trx['hour'] > df_trx['slot_max']),
            df_trx['slot_max'],
            df_trx['hour'],
        ),
    ).astype(int)

    df_trx_fixed = (
        df_trx
        .groupby(['rider_code', 'date', 'week', 'hour', 'payment_type'])
        .agg(
            total_orders=('total_orders', 'sum'),
            total_qty=('total_qty', 'sum'),
            total_sales=('total_sales', 'sum'),
        )
        .reset_index()
    )

    # ── Step 3b: Case 2 — synthesise slots for riders with no TI-TO data ─────
    case2_rider_dates = (
        df_trx_fixed
        .groupby(['rider_code', 'date'])['hour']
        .agg(hour_min='min', hour_max='max')
        .reset_index()
        .merge(df_slot_range[['rider_code', 'date']], on=['rider_code', 'date'], how='left', indicator=True)
        .query('_merge == "left_only"')
        .drop(columns='_merge')
    )

    case2_slots_list = []
    for _, row in case2_rider_dates.iterrows():
        for h in range(int(row['hour_min']), int(row['hour_max']) + 1):
            case2_slots_list.append({
                'rider_code': row['rider_code'],
                'date': row['date'],
                'hour_slot': h,
            })

    if case2_slots_list:
        df_case2_slots = pd.DataFrame(case2_slots_list)
        df_case2_slots['date'] = pd.to_datetime(df_case2_slots['date'])
        df_timeslot_fixed = (
            pd.concat([df_timeslot, df_case2_slots], ignore_index=True)
            .drop_duplicates()
        )
    else:
        df_timeslot_fixed = df_timeslot.copy()

    # ── Step 3c: Case 3 — exclude rider+dates with zero daily transactions ───
    rider_dates_with_trx = df_trx_fixed[['rider_code', 'date']].drop_duplicates()
    df_timeslot_base = df_timeslot_fixed.merge(
        rider_dates_with_trx,
        on=['rider_code', 'date'],
        how='inner',
    )

    # ── Step 4: Final merge — hour_slot as the base, join transactions ────────
    df_final = df_timeslot_base.merge(
        df_trx_fixed,
        left_on=['rider_code', 'date', 'hour_slot'],
        right_on=['rider_code', 'date', 'hour'],
        how='left',
    )

    # ── Step 5: Fill week column for slot rows that had no matching transaction
    df_final['week'] = df_final['week'].fillna(
        pd.to_datetime(df_final['date']).dt.isocalendar().week.astype(int)
    )

    # ── Step 6: Join rider mapping ─────────────────────────────────────────────
    df_final_merged = df_final.merge(
        df_rider_mapping,
        left_on='rider_code',
        right_on='rider_id',
        how='left',
    ).drop(columns=['rider_code'])

    df_final_merged = df_final_merged[[
        'rider_id', 'jilid', 'hub_name', 'region', 'provinsi', 'kota', 'sejuta_jiwa_type',
        'date', 'checkin', 'checkout', 'hour_slot', 'week', 'hour',
        'payment_type', 'total_orders', 'total_qty', 'total_sales',
    ]]

    # ── Step 7: Strip whitespace then replace empty-like strings with NaN ─────
    df_final_merged = df_final_merged.apply(
        lambda col: col.map(lambda x: x.strip() if isinstance(x, str) else x)
    )
    df_final_merged = df_final_merged.replace(
        {'': np.nan, 'None': np.nan, 'nan': np.nan, 'null': np.nan, 'NULL': np.nan}
    )

    df_final_merged['load_data_by'] = load_data_by

    return df_final_merged
