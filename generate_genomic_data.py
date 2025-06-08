import pandas as pd
import random
import os

# Constants
GENES = ["16S_rRNA", "gyrB", "recA", "rpoB", "dnaK", "atpD"]
TAX_IDS = [101, 102, 103, 104]

def random_dna(length=200):
    return ''.join(random.choices("ACGT", k=length))

def generate_data(n=100):
    rows = []
    for i in range(1, n + 1):
        start = random.randint(1, 1_000_000)
        seq_len = random.randint(100, 300)
        rows.append({
            "genome_id": f"G{str(i).zfill(4)}",
            "gene_name": random.choice(GENES),
            "sequence": random_dna(seq_len),
            "start_position": start,
            "end_position": start + seq_len,
            "gtdb_taxonomy_id": random.choice(TAX_IDS)
        })
    return pd.DataFrame(rows)

def save_data(df):
    os.makedirs("output", exist_ok=True)
    df.to_parquet("output/genomic_data.parquet", index=False)
    df.to_json("output/genomic_data.json", orient="records", lines=True)

if __name__ == "__main__":
    df = generate_data()
    save_data(df)
    print("✅ Genomic data saved to 'output/genomic_data.parquet' and '.json'")
