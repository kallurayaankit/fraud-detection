import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
import joblib

# Generate synthetic transaction data
np.random.seed(42)
n = 5000
data = {
    'amount': np.random.exponential(scale=100, size=n),
    'transaction_hour': np.random.randint(0, 24, n),
    'merchant_category': np.random.randint(0, 10, n),
    'distance_from_home': np.random.exponential(scale=10, size=n),
    'is_fraud': np.random.choice([0,1], n, p=[0.98, 0.02])
}
df = pd.DataFrame(data)

X = df.drop('is_fraud', axis=1)
y = df['is_fraud']

model = RandomForestClassifier(n_estimators=100, random_state=42)
model.fit(X, y)

# Save model
joblib.dump(model, 'model/model.pkl')
print("Model saved to model/model.pkl")

# Save a reference sample for drift monitoring
reference_sample = df.sample(n=1000, random_state=42)
reference_sample.to_csv('reference_data.csv', index=False)
print("Reference data saved to reference_data.csv")