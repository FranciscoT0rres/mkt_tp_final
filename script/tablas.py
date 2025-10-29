"""tablas.py
Construye dimensiones (6) y hechos (6) a partir de los archivos en warehouse/staging.

Dimensiones creadas (surrogate keys):
- dim_customers
- dim_products
- dim_stores
- dim_date
- dim_address
- dim_product_category

Hechos creados:
- fact_order_lines
- fact_orders
- fact_payments
- fact_shipments
- fact_web_sessions
- fact_nps

El script intenta leer parquet (.parquet) primero, cae a CSV si no existe.
"""
from pathlib import Path
import pandas as pd
import sys

PROJECT_ROOT = Path(__file__).resolve().parent.parent
STAGING_DIR = PROJECT_ROOT / "warehouse" / "staging"
DWH_DIR = PROJECT_ROOT / "warehouse"


def read_staging(name):
    p_par = STAGING_DIR / f"{name}.parquet"
    p_csv = STAGING_DIR / f"{name}.csv"
    if p_par.exists():
        return pd.read_parquet(p_par)
    if p_csv.exists():
        return pd.read_csv(p_csv, encoding='utf-8', low_memory=False)
    print(f"[warn] staging {name} no encontrado en {STAGING_DIR}", file=sys.stderr)
    return None


def write_table(df, name):
    if df is None or df.empty:
        print(f"[info] tabla {name} vacía, no se guarda.")
        return
    out = DWH_DIR / f"{name}.parquet"
    try:
        df.to_parquet(out, index=False)
        print(f"[ok] guardado {out} rows={len(df)}")
    except Exception as e:
        # fallback to csv if parquet engine missing
        try:
            out_csv = DWH_DIR / f"{name}.csv"
            df.to_csv(out_csv, index=False, encoding='utf-8')
            print(f"[ok] guardado {out_csv} rows={len(df)} (csv fallback: {e})")
        except Exception as ex:
            print(f"[error] no se pudo guardar {name}: {ex}", file=sys.stderr)


def _ensure_datetime(df, col):
    if col in df.columns:
        df[col] = pd.to_datetime(df[col], errors='coerce')


def build_dimensions():
    dims = {}

    # dim_customers
    customers = read_staging('customers')
    if customers is not None:
        c = customers.copy()
        # canonical columns
        if 'customer_id' not in c.columns and 'id' in c.columns:
            c = c.rename(columns={'id': 'customer_id'})
        _ensure_datetime(c, 'created_at')
        # dedupe on natural key
        if 'customer_id' in c.columns:
            c = c.drop_duplicates(subset=['customer_id']).reset_index(drop=True)
        else:
            c = c.drop_duplicates().reset_index(drop=True)
        c.insert(0, 'customer_sk', range(1, len(c)+1))
        write_table(c, 'dim_customers')
        dims['customers'] = c
    else:
        dims['customers'] = None

    # dim_products
    products = read_staging('products')
    if products is not None:
        p = products.copy()
        _ensure_datetime(p, 'created_at')
        if 'product_id' in p.columns:
            p = p.drop_duplicates(subset=['product_id']).reset_index(drop=True)
        else:
            p = p.drop_duplicates().reset_index(drop=True)
        p.insert(0, 'product_sk', range(1, len(p)+1))
        write_table(p, 'dim_products')
        dims['products'] = p
    else:
        dims['products'] = None

    # dim_stores
    stores = read_staging('stores')
    if stores is None:
        stores = read_staging('channels')
    if stores is not None:
        s = stores.copy()
        # some staging use channel_id as natural key
        if 'store_id' not in s.columns and 'channel_id' in s.columns:
            s = s.rename(columns={'channel_id': 'store_id'})
        if 'store_id' in s.columns:
            s = s.drop_duplicates(subset=['store_id']).reset_index(drop=True)
        else:
            s = s.drop_duplicates().reset_index(drop=True)
        s.insert(0, 'store_sk', range(1, len(s)+1))
        write_table(s, 'dim_stores')
        dims['stores'] = s
    else:
        dims['stores'] = None

    # dim_date - build from any date columns in staging (orders.created_at/order_date, customers.created_at, products.created_at)
    dates = None
    orders = read_staging('orders')
    if orders is not None and 'order_date' in orders.columns:
        dates = pd.DataFrame({'date': pd.to_datetime(orders['order_date'], errors='coerce').dt.normalize().dropna().unique()})
    # fallback: customers.created_at
    if dates is None or dates.empty:
        if customers is not None and 'created_at' in customers.columns:
            dates = pd.DataFrame({'date': pd.to_datetime(customers['created_at'], errors='coerce').dt.normalize().dropna().unique()})
    if dates is None or dates.empty:
        # create small default calendar from min/max of orders if available
        if orders is not None and 'order_date' in orders.columns:
            rng = pd.to_datetime(orders['order_date'], errors='coerce')
            mn = rng.min()
            mx = rng.max()
            if pd.notna(mn) and pd.notna(mx):
                dates = pd.DataFrame({'date': pd.date_range(mn.normalize(), mx.normalize(), freq='D')})
    if dates is not None and not dates.empty:
        dates = dates.sort_values('date').reset_index(drop=True)
        dates['date'] = pd.to_datetime(dates['date'])
        dates.insert(0, 'date_sk', range(1, len(dates)+1))
        dates['date_id'] = dates['date'].dt.strftime('%Y%m%d').astype(int)
        dates['year'] = dates['date'].dt.year
        dates['month'] = dates['date'].dt.month
        dates['day'] = dates['date'].dt.day
        dates['weekday'] = dates['date'].dt.weekday
        write_table(dates, 'dim_date')
        dims['dates'] = dates
    else:
        dims['dates'] = None

    # dim_address
    address = read_staging('address')
    if address is not None:
        a = address.copy()
        if 'address_id' in a.columns:
            a = a.drop_duplicates(subset=['address_id']).reset_index(drop=True)
        else:
            a = a.drop_duplicates().reset_index(drop=True)
        a.insert(0, 'address_sk', range(1, len(a)+1))
        write_table(a, 'dim_address')
        dims['address'] = a
    else:
        dims['address'] = None

    # dim_product_category
    pc = read_staging('product_category')
    if pc is not None:
        cat = pc.copy()
        # assume category_id exists
        if 'category_id' in cat.columns:
            cat = cat.drop_duplicates(subset=['category_id']).reset_index(drop=True)
        else:
            cat = cat.drop_duplicates().reset_index(drop=True)
        cat.insert(0, 'product_category_sk', range(1, len(cat)+1))
        write_table(cat, 'dim_product_category')
        dims['product_category'] = cat
    else:
        dims['product_category'] = None

    return dims


def build_facts(dims):
    # fact_order_lines
    items = read_staging('order_items')
    if items is None:
        items = read_staging('sales_order_item')
    if items is None:
        items = read_staging('sales_order_item')

    orders = read_staging('orders')
    if orders is None:
        orders = read_staging('sales_order')
    if items is not None:
        f = items.copy()
        # merge order header to get order_date, customer_id, store_id
        if orders is not None and 'order_id' in orders.columns:
            f = f.merge(orders[['order_id', 'order_date', 'customer_id', 'store_id']], on='order_id', how='left')

        # ensure quantity and price
        if 'quantity' not in f.columns and 'qty' in f.columns:
            f['quantity'] = f['qty']
        if 'unit_price' not in f.columns and 'price' in f.columns:
            f['unit_price'] = f['price']

        # map to surrogate keys from dims
        if dims.get('customers') is not None and 'customer_id' in f.columns:
            f = f.merge(dims['customers'][['customer_sk', 'customer_id']], on='customer_id', how='left')
        if dims.get('products') is not None and 'product_id' in f.columns:
            f = f.merge(dims['products'][['product_sk', 'product_id']], on='product_id', how='left')
        if dims.get('stores') is not None and 'store_id' in f.columns:
            f = f.merge(dims['stores'][['store_sk', 'store_id']], on='store_id', how='left')

        # date dimension mapping
        if dims.get('dates') is not None:
            if 'order_date' in f.columns:
                f['order_date'] = pd.to_datetime(f['order_date'], errors='coerce')
                f = f.merge(dims['dates'][['date_sk', 'date', 'date_id']], left_on=pd.to_datetime(f['order_date']).dt.normalize(), right_on='date', how='left')
                # keep date_sk
                if 'date_sk' in f.columns:
                    f = f.rename(columns={'date_sk': 'order_date_sk'})
                # drop helper cols
                f = f.drop(columns=[col for col in ('key_0','date') if col in f.columns])

        # compute measures
        if 'quantity' not in f.columns:
            f['quantity'] = 1
        if 'unit_price' in f.columns:
            f['line_total'] = f['quantity'] * f['unit_price']
        else:
            if 'line_total' not in f.columns:
                f['line_total'] = None

        # select common fact columns
        cols = [c for c in ['order_id', 'order_date_sk', 'customer_sk', 'product_sk', 'store_sk', 'quantity', 'unit_price', 'line_total'] if c in f.columns]
        fact_lines = f[cols].copy().drop_duplicates().reset_index(drop=True)
        write_table(fact_lines, 'fact_order_lines')
    else:
        print('[warn] no hay order_items para crear fact_order_lines')

    # fact_orders
    if orders is not None:
        o = orders.copy()
        # map customer and store
        if dims.get('customers') is not None and 'customer_id' in o.columns:
            o = o.merge(dims['customers'][['customer_sk', 'customer_id']], on='customer_id', how='left')
        if dims.get('stores') is not None and 'store_id' in o.columns:
            o = o.merge(dims['stores'][['store_sk', 'store_id']], on='store_id', how='left')
        # map date
        if dims.get('dates') is not None and 'order_date' in o.columns:
            o['order_date'] = pd.to_datetime(o['order_date'], errors='coerce')
            o = o.merge(dims['dates'][['date_sk','date']], left_on=pd.to_datetime(o['order_date']).dt.normalize(), right_on='date', how='left')
            if 'date_sk' in o.columns:
                o = o.rename(columns={'date_sk': 'order_date_sk'})
            o = o.drop(columns=[col for col in ('key_0','date') if col in o.columns])

        # select
        cols = [c for c in ['order_id','order_date_sk','customer_sk','store_sk','status','subtotal','tax_amount','shipping_fee','total_amount'] if c in o.columns]
        fact_orders = o[cols].copy().drop_duplicates().reset_index(drop=True)
        write_table(fact_orders, 'fact_orders')
    else:
        print('[warn] no hay orders para crear fact_orders')

    # fact_payments
    payments = read_staging('payment')
    if payments is not None:
        pay = payments.copy()
        if 'order_id' in pay.columns and dims.get('dates') is not None and 'created_at' in pay.columns:
            pay['created_at'] = pd.to_datetime(pay['created_at'], errors='coerce')
            pay = pay.merge(dims['dates'][['date_sk','date']], left_on=pd.to_datetime(pay['created_at']).dt.normalize(), right_on='date', how='left')
            if 'date_sk' in pay.columns:
                pay = pay.rename(columns={'date_sk':'payment_date_sk'})
            pay = pay.drop(columns=[col for col in ('key_0','date') if col in pay.columns])
        cols = [c for c in ['payment_id','order_id','payment_date_sk','amount','status','payment_method'] if c in pay.columns]
        fact_pay = pay[cols].copy().drop_duplicates().reset_index(drop=True)
        write_table(fact_pay, 'fact_payments')
    else:
        print('[warn] no hay payment para crear fact_payments')

    # fact_shipments
    shipments = read_staging('shipment')
    if shipments is not None:
        s = shipments.copy()
        if 'order_id' in s.columns and dims.get('dates') is not None and 'shipped_at' in s.columns:
            s['shipped_at'] = pd.to_datetime(s['shipped_at'], errors='coerce')
            s = s.merge(dims['dates'][['date_sk','date']], left_on=pd.to_datetime(s['shipped_at']).dt.normalize(), right_on='date', how='left')
            if 'date_sk' in s.columns:
                s = s.rename(columns={'date_sk':'shipped_date_sk'})
            s = s.drop(columns=[col for col in ('key_0','date') if col in s.columns])
        cols = [c for c in ['shipment_id','order_id','shipped_date_sk','carrier','status','tracking_number'] if c in s.columns]
        fact_ship = s[cols].copy().drop_duplicates().reset_index(drop=True)
        write_table(fact_ship, 'fact_shipments')
    else:
        print('[warn] no hay shipment para crear fact_shipments')

    # fact_web_sessions
    ws = read_staging('web_session')
    if ws is not None:
        w = ws.copy()
        if 'started_at' in w.columns and dims.get('dates') is not None:
            w['started_at'] = pd.to_datetime(w['started_at'], errors='coerce')
            w = w.merge(dims['dates'][['date_sk','date']], left_on=pd.to_datetime(w['started_at']).dt.normalize(), right_on='date', how='left')
            if 'date_sk' in w.columns:
                w = w.rename(columns={'date_sk':'session_date_sk'})
            w = w.drop(columns=[col for col in ('key_0','date') if col in w.columns])
        cols = [c for c in ['session_id','customer_id','session_date_sk','page_views','duration_seconds'] if c in w.columns]
        fact_ws = w[cols].copy().drop_duplicates().reset_index(drop=True)
        write_table(fact_ws, 'fact_web_sessions')
    else:
        print('[warn] no hay web_session para crear fact_web_sessions')

    # fact_nps
    nps = read_staging('nps_response')
    if nps is not None:
        n = nps.copy()
        if 'response_date' in n.columns and dims.get('dates') is not None:
            n['response_date'] = pd.to_datetime(n['response_date'], errors='coerce')
            n = n.merge(dims['dates'][['date_sk','date']], left_on=pd.to_datetime(n['response_date']).dt.normalize(), right_on='date', how='left')
            if 'date_sk' in n.columns:
                n = n.rename(columns={'date_sk':'response_date_sk'})
            n = n.drop(columns=[col for col in ('key_0','date') if col in n.columns])
        cols = [c for c in ['nps_id','customer_id','response_date_sk','score','comment'] if c in n.columns]
        fact_nps = n[cols].copy().drop_duplicates().reset_index(drop=True)
        write_table(fact_nps, 'fact_nps')
    else:
        print('[warn] no hay nps_response para crear fact_nps')


def main():
    print(f"[start] STAGING_DIR={STAGING_DIR} -> DWH_DIR={DWH_DIR}")
    dims = build_dimensions()
    build_facts(dims)
    print('[done] tablas creadas')


if __name__ == '__main__':
    main()
