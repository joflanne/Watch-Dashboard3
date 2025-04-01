def detect_fake(row):
    if 'no serial' in row['Description'].lower() or 'replica' in row['Description'].lower() or row['Price'] < 100:
        return 'Likely Fake'
    market_prices = {'Rolex Submariner': 8000, 'Seiko Prospex': 400, 'Omega Speedmaster': 3000}  # Mock data
    key = f"{row['Brand']} {row['Model']}"
    if key in market_prices and row['Price'] < market_prices[key] * 0.3:
        return 'Likely Fake'
    return 'Likely Genuine'