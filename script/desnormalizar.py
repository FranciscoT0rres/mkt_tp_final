# ...existing code...
"""
Script: desnormalizar.py
Función: desde los archivos raw genera tablas desnormalizadas (dimensiones + fact) y las guarda en /warehouse.
Requisitos: pandas, pyarrow (para parquet). Ejecutar en Windows desde la carpeta del proyecto.
"""
from pathlib import Path
import pandas as pd
import sys

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "raw"
DWH_DIR = PROJECT_ROOT / "warehouse"
DWH_DIR.mkdir(parents=True, exist_ok=True)

# staging dir
STAGING_DIR = DWH_DIR / "staging"
STAGING_DIR.mkdir(parents=True, exist_ok=True)

def find_file(base_name):
    """
    Busca base_name.csv o base_name.parquet en RAW_DIR y devuelve la Path, o None.
    """
    for ext in (".parquet", ".csv"):
        p = RAW_DIR / f"{base_name}{ext}"
        if p.exists():
            return p
    return None

def read_table(name, **read_kwargs):
    p = find_file(name)
    if not p:
        print(f"[warn] no se encontró {name} en {RAW_DIR}", file=sys.stderr)
        return None
    if p.suffix == ".parquet":
        return pd.read_parquet(p, **read_kwargs)
    else:
        return pd.read_csv(p, **read_kwargs)


def _canonicalize_columns(df):
    """Lowercase, strip spaces and replace spaces with underscore for staging."""
    df = df.copy()
    df.columns = [c.strip().lower().replace(' ', '_') if isinstance(c, str) else c for c in df.columns]
    return df


def write_staging(df, name):
    """Write a staging parquet file under warehouse/staging."""
    if df is None or df.empty:
        print(f"[info] staging {name} vacía, no se guarda.")
        return
    out = STAGING_DIR / f"{name}.parquet"
    df.to_parquet(out, index=False)
    print(f"[ok] guardado staging {out} rows={len(df)}")


def build_staging():
    """Read raw files, normalize column names and types, and write staging parquet tables.

    Creates staging tables for: customers, products, stores (or channels), orders, order_items, dates, payment, shipment, store, web_session, nps_response.
    """
    # Build a map from actual raw filenames to canonical staging names
    raw_files = list(RAW_DIR.glob("*.csv")) + list(RAW_DIR.glob("*.parquet"))
    if not raw_files:
        print(f"[warn] no hay archivos en {RAW_DIR}", file=sys.stderr)
        return False

    # simple mapping rules: filename contains keyword -> canonical name
    mapping_rules = {
        'customer': 'customers',
        'product': 'products',
        'store': 'stores',
        'channel': 'stores',
        'order_item': 'order_items',
        'sales_order_item': 'order_items',
        'sales_order': 'orders',
        'sales_order_item': 'order_items',
        'order': 'orders',
        'payment': 'payment',
        'shipment': 'shipment',
        'web_session': 'web_session',
        'nps_response': 'nps_response',
        'province': 'province',
        'product_category': 'product_category',
        'address': 'address'
    }

    processed = set()

    for p in raw_files:
        name = p.stem.lower()
        # find best mapping
        target = None
        for k, v in mapping_rules.items():
            if k in name:
                target = v
                break
        if target is None:
            target = name

        if target in processed:
            print(f"[info] ya procesado {target}, salto {p.name}")
            continue

        try:
            if p.suffix == '.parquet':
                df = pd.read_parquet(p)
            else:
                # robust CSV read
                df = pd.read_csv(p, encoding='utf-8', low_memory=False)
        except Exception as e:
            print(f"[warn] no se pudo leer {p.name}: {e}", file=sys.stderr)
            continue

        # canonicalize columns
        df = _canonicalize_columns(df)

        # try to parse common date columns
        for c in list(df.columns):
            if c in ('order_date', 'created_at', 'fecha', 'date') or c.endswith('_date') or c.endswith('_at'):
                try:
                    df[c] = pd.to_datetime(df[c], errors='coerce')
                except Exception:
                    pass

        # safe write: try parquet, fallback to csv if pyarrow missing
        try:
            write_staging(df, target)
        except Exception as e:
            try:
                out = STAGING_DIR / f"{target}.csv"
                df.to_csv(out, index=False, encoding='utf-8')
                print(f"[ok] guardado staging {out} rows={len(df)} (csv fallback)")
            except Exception as ex:
                print(f"[error] no se pudo guardar staging {target}: {ex}", file=sys.stderr)

        processed.add(target)

    return True

def write_table(df, name):
    if df is None or df.empty:
        print(f"[info] tabla {name} vacía, no se guarda.")
        return
    out = DWH_DIR / f"{name}.parquet"
    df.to_parquet(out, index=False)
    print(f"[ok] guardado {out} rows={len(df)}")

def build_dimensions():
    # Clientes
    customers = read_table("customers")
    if customers is not None:
        customers_dim = customers.drop_duplicates().reset_index(drop=True)
        # normalizar nombres y key surrogate si no existe
        if "customer_id" not in customers_dim.columns:
            customers_dim.insert(0, "customer_id", range(1, len(customers_dim)+1))
        write_table(customers_dim, "dim_customers")
    else:
        customers_dim = None

    # Productos
    products = read_table("products")
    if products is not None:
        products_dim = products.drop_duplicates().reset_index(drop=True)
        if "product_id" not in products_dim.columns:
            products_dim.insert(0, "product_id", range(1, len(products_dim)+1))
        write_table(products_dim, "dim_products")
    else:
        products_dim = None

    # Tiendas / canales
    stores = read_table("stores") or read_table("channels")
    if stores is not None:
        stores_dim = stores.drop_duplicates().reset_index(drop=True)
        if "store_id" not in stores_dim.columns:
            stores_dim.insert(0, "store_id", range(1, len(stores_dim)+1))
        write_table(stores_dim, "dim_stores")
    else:
        stores_dim = None

    # Fechas: generar calendario si no existe
    dates = read_table("dates")
    if dates is None:
        # intentar extraer fechas desde orders
        orders = read_table("orders")
        if orders is not None and "order_date" in orders.columns:
            dates = pd.DataFrame({"date": pd.to_datetime(orders["order_date"]).dt.date.unique()})
            dates = dates.sort_values("date").reset_index(drop=True)
            dates["date_id"] = range(1, len(dates)+1)
        else:
            dates = None

    if dates is not None:
        # homogeneizar nombre de columna
        if "date_id" not in dates.columns:
            if "date" not in dates.columns:
                # intentar columnas comunes
                for c in ("order_date", "fecha"):
                    if c in dates.columns:
                        dates = dates.rename(columns={c: "date"})
                        break
            dates["date"] = pd.to_datetime(dates["date"])
            dates["date_id"] = dates["date"].dt.strftime("%Y%m%d").astype(int)
            dates["year"] = dates["date"].dt.year
            dates["month"] = dates["date"].dt.month
            dates["day"] = dates["date"].dt.day
        write_table(dates, "dim_date")
    else:
        dates = None

    return {
        "customers": customers_dim,
        "products": products_dim,
        "stores": stores_dim,
        "dates": dates
    }

def build_fact_order_lineitems(dims):
    """
    Construye fact_orders (a nivel de línea de pedido) juntando orders + order_items + dimensiones.
    """
    orders = read_table("orders")
    items = read_table("order_items") or read_table("items") or read_table("orderlines")
    if orders is None or items is None:
        print("[error] faltan orders o order_items, fact no creado.", file=sys.stderr)
        return

    # homogeneizar claves
    # suponer order_id, product_id, customer_id, store_id, order_date, quantity, price
    df = items.merge(orders, on="order_id", how="left", suffixes=("_item", "_order"))

    # merge dimensiones si están disponibles
    if dims.get("customers") is not None and "customer_id" in df.columns:
        df = df.merge(dims["customers"], on="customer_id", how="left", suffixes=("", "_cust"))
    if dims.get("products") is not None and "product_id" in df.columns:
        df = df.merge(dims["products"], on="product_id", how="left", suffixes=("", "_prod"))
    if dims.get("stores") is not None and "store_id" in df.columns:
        df = df.merge(dims["stores"], on="store_id", how="left", suffixes=("", "_store"))
    if dims.get("dates") is not None:
        # crear date_id desde order_date si existe
        if "order_date" in df.columns:
            df["order_date"] = pd.to_datetime(df["order_date"])
            df["date_id"] = df["order_date"].dt.strftime("%Y%m%d").astype(int)
        elif "date" in df.columns:
            df["date_id"] = pd.to_datetime(df["date"]).dt.strftime("%Y%m%d").astype(int)

    # calcular medidas
    # buscar columnas de cantidad y precio
    qty_col = next((c for c in df.columns if c.lower() in ("quantity", "qty", "units")), None)
    price_col = next((c for c in df.columns if c.lower() in ("price", "unit_price", "unitprice", "precio")), None)
    df["quantity"] = df[qty_col] if qty_col in df.columns else 1
    if price_col in df.columns:
        df["unit_price"] = df[price_col]
        df["line_total"] = df["quantity"] * df["unit_price"]
    else:
        df["unit_price"] = None
        df["line_total"] = None

    # seleccionar columnas relevantes para el fact
    preserve = ["order_id", "date_id", "customer_id", "product_id", "store_id", "quantity", "unit_price", "line_total"]
    preserve = [c for c in preserve if c in df.columns]
    fact = df[preserve].copy().drop_duplicates().reset_index(drop=True)
    write_table(fact, "fact_order_lines")
    return fact

def main():
    print(f"[start] RAW_DIR={RAW_DIR} -> DWH_DIR={DWH_DIR}")
    # primero generar staging normalizado
    build_staging()
    dims = build_dimensions()
    build_fact_order_lineitems(dims)
    print("[done] proceso terminado.")

if __name__ == "__main__":
    main()
