from Scripts import bronze_layer, silver_layer, gold_layer


if __name__ == "__main__":
    print("▶️ Starting Bronze Layer...")
    bronze_layer.main()
    print("✅ Bronze Layer completed.\n")

    print("▶️ Starting Silver Layer...")
    silver_layer.main()
    print("✅ Silver Layer completed.\n")

    print("▶️ Starting Gold Layer...")
    gold_layer.main()
    print("✅ Gold Layer completed.\n")

    print("🎉 All layers executed successfully.")
