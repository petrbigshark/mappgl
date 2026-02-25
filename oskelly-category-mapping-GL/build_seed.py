\
import argparse
import pandas as pd

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="Excel with columns: expr, outputValue (your 3000+ mappings)")
    ap.add_argument("--out", required=True, help="Output seed csv")
    args = ap.parse_args()

    df = pd.read_excel(args.input)
    if df.shape[1] < 2:
        raise SystemExit("Need at least two columns")
    expr_col = df.columns[0]
    out_col = df.columns[1]
    df = df[[expr_col, out_col]].rename(columns={expr_col: "expr", out_col: "outputValue"})
    df["expr"] = df["expr"].astype(str).str.strip()
    df["outputValue"] = df["outputValue"].astype(str).str.strip()

    freq = df.groupby(["expr","outputValue"]).size().reset_index(name="n")
    tot = freq.groupby("expr")["n"].sum().reset_index(name="count_total")
    nu = freq.groupby("expr")["outputValue"].nunique().reset_index(name="n_unique")

    m = freq.merge(tot, on="expr", how="left")
    m["confidence"] = m["n"] / m["count_total"]
    m = m.sort_values(["expr","confidence","n"], ascending=[True, False, False])
    top = m.groupby("expr").head(1).merge(nu, on="expr", how="left")
    top["is_conflict"] = top["n_unique"] > 1
    top = top.rename(columns={"n":"count_top"}).drop(columns=["n_unique"])

    top.to_csv(args.out, index=False, encoding="utf-8-sig")
    print(f"OK: wrote {len(top)} rows to {args.out}")

if __name__ == "__main__":
    main()
