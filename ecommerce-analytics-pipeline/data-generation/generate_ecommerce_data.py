import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime, timedelta
import os

fake = Faker()
Faker.seed(42)
np.random.seed(42)
random.seed(42)

class EcommerceDataGenerator:
    def __init__(self):
        self.start_date = datetime(2022, 1, 1)
        self.end_date = datetime(2024, 12, 31)
        
    def generate_customers(self, num_customers=500000):  # Increased default
        """Generate customer data with realistic demographics"""
        customers = []
        
        for i in range(num_customers):
            # Create customer segments
            segment = np.random.choice(['premium', 'regular', 'budget'], 
                                     p=[0.15, 0.60, 0.25])
            
            customer = {
                'customer_id': i + 1,
                'first_name': fake.first_name(),
                'last_name': fake.last_name(),
                'email': fake.email(),
                'phone': fake.phone_number(),
                'address': fake.address().replace('\n', ', '),
                'city': fake.city(),
                'state': fake.state_abbr(),
                'zip_code': fake.zipcode(),
                'country': 'USA',
                'registration_date': fake.date_between(
                    start_date=self.start_date, 
                    end_date=self.end_date
                ),
                'customer_segment': segment,
                'birth_date': fake.date_of_birth(minimum_age=18, maximum_age=80),
                'gender': np.random.choice(['M', 'F', 'O'], p=[0.48, 0.48, 0.04])
            }
            customers.append(customer)
        
        return pd.DataFrame(customers)
    
    def generate_products(self, num_products=50000):  # Increased default
        """Generate product catalog with categories and pricing"""
        categories = [
            'Electronics', 'Clothing', 'Home & Garden', 'Sports', 
            'Books', 'Beauty', 'Toys', 'Automotive', 'Food', 'Health'
        ]
        
        products = []
        
        for i in range(num_products):
            category = np.random.choice(categories)
            
            # Price distribution based on category
            price_ranges = {
                'Electronics': (50, 2000),
                'Clothing': (15, 200),
                'Home & Garden': (20, 500),
                'Sports': (25, 300),
                'Books': (10, 50),
                'Beauty': (15, 150),
                'Toys': (10, 100),
                'Automotive': (25, 1000),
                'Food': (5, 100),
                'Health': (10, 200)
            }
            
            min_price, max_price = price_ranges[category]
            base_price = np.random.uniform(min_price, max_price)
            
            product = {
                'product_id': i + 1,
                'product_name': fake.catch_phrase(),
                'category': category,
                'subcategory': f"{category} - {fake.word().title()}",
                'brand': fake.company(),
                'price': round(base_price, 2),
                'cost': round(base_price * np.random.uniform(0.4, 0.7), 2),
                'weight': round(np.random.uniform(0.1, 10), 2),
                'dimensions': f"{np.random.randint(5, 50)}x{np.random.randint(5, 50)}x{np.random.randint(2, 20)}",
                'description': fake.text(max_nb_chars=200),
                'created_date': fake.date_between(
                    start_date=self.start_date, 
                    end_date=self.end_date
                ),
                'is_active': np.random.choice([True, False], p=[0.85, 0.15])
            }
            products.append(product)
        
        return pd.DataFrame(products)
    
    def generate_orders(self, customers_df, products_df, num_orders=5000000):  # Increased default
        """Generate order data with realistic patterns"""
        orders = []
        
        # Customer behavior patterns
        customer_segments = customers_df.set_index('customer_id')['customer_segment'].to_dict()
        
        for i in range(num_orders):
            # Select customer with segment-based probability
            customer_id = np.random.choice(customers_df['customer_id'].values)
            segment = customer_segments[customer_id]
            
            # Order frequency based on segment
            if segment == 'premium':
                order_prob = 0.15
            elif segment == 'regular':
                order_prob = 0.08
            else:  # budget
                order_prob = 0.05
            
            # Generate order date with seasonal patterns
            order_date = fake.date_between(
                start_date=self.start_date, 
                end_date=self.end_date
            )
            
            # Add seasonal boost (holiday shopping)
            if order_date.month in [11, 12]:  # November, December
                seasonal_boost = 1.5
            elif order_date.month in [6, 7]:  # Summer
                seasonal_boost = 1.2
            else:
                seasonal_boost = 1.0
            
            # Order status distribution
            order_status = np.random.choice([
                'completed', 'cancelled', 'returned', 'shipped', 'pending'
            ], p=[0.75, 0.10, 0.05, 0.08, 0.02])
            
            order = {
                'order_id': i + 1,
                'customer_id': customer_id,
                'order_date': order_date,
                'order_status': order_status,
                'payment_method': np.random.choice([
                    'credit_card', 'debit_card', 'paypal', 'apple_pay', 'google_pay'
                ], p=[0.45, 0.25, 0.15, 0.08, 0.07]),
                'shipping_method': np.random.choice([
                    'standard', 'express', 'overnight', 'pickup'
                ], p=[0.60, 0.25, 0.10, 0.05]),
                'shipping_cost': round(np.random.uniform(0, 25), 2),
                'tax_amount': 0.0,  # Fixed: Initialize as float
                'total_amount': 0.0,  # Fixed: Initialize as float
                'currency': 'USD',
                'created_at': fake.date_time_between(
                    start_date=order_date, 
                    end_date=order_date + timedelta(hours=1)
                ),
                'updated_at': fake.date_time_between(
                    start_date=order_date, 
                    end_date=order_date + timedelta(days=7)
                )
            }
            orders.append(order)
        
        # Convert to DataFrame with proper dtypes
        orders_df = pd.DataFrame(orders)
        orders_df['tax_amount'] = orders_df['tax_amount'].astype(float)
        orders_df['total_amount'] = orders_df['total_amount'].astype(float)
        
        return orders_df
    
    def generate_order_items_optimized(self, orders_df, products_df):
        """Generate order items with optimized approach"""
        print("Generating order items (this may take a while for large datasets)...")
        
        # Pre-calculate everything we need
        products_dict = products_df.set_index('product_id')[['price']].to_dict('index')
        product_ids = products_df['product_id'].values
        
        # Create arrays for vectorized operations
        order_ids = orders_df['order_id'].values
        order_created_at = orders_df['created_at'].values
        
        # Pre-generate all random choices for better performance
        num_items_per_order = np.random.choice([1, 2, 3, 4, 5], 
                                             size=len(orders_df), 
                                             p=[0.50, 0.30, 0.12, 0.05, 0.03])
        
        order_items = []
        order_totals = {}
        
        for idx, (order_id, created_at, num_items) in enumerate(zip(order_ids, order_created_at, num_items_per_order)):
            # Select products for this order
            selected_products = np.random.choice(product_ids, size=num_items, replace=False)
            
            order_total = 0
            
            for product_id in selected_products:
                product = products_dict[product_id]
                
                # Quantity (1-3 for most items)
                quantity = np.random.choice([1, 2, 3], p=[0.70, 0.20, 0.10])
                
                # Price with possible discounts
                unit_price = product['price']
                discount_amount = 0
                if np.random.random() < 0.15:  # 15% chance of discount
                    discount_pct = np.random.uniform(0.10, 0.30)
                    discount_amount = unit_price * quantity * discount_pct
                    unit_price = unit_price * (1 - discount_pct)
                
                line_total = round(unit_price * quantity, 2)
                order_total += line_total
                
                order_item = {
                    'order_item_id': len(order_items) + 1,
                    'order_id': order_id,
                    'product_id': product_id,
                    'quantity': quantity,
                    'unit_price': round(unit_price, 2),
                    'line_total': line_total,
                    'discount_amount': round(discount_amount, 2),
                    'created_at': created_at
                }
                order_items.append(order_item)
            
            order_totals[order_id] = order_total
            
            # Print progress for large datasets
            if idx % 100000 == 0 and idx > 0:
                print(f"Processed {idx:,} orders...")
        
        # Update order totals efficiently
        print("Updating order totals...")
        tax_rate = 0.08
        
        for order_id, subtotal in order_totals.items():
            tax_amount = round(subtotal * tax_rate, 2)
            shipping_cost = orders_df.loc[orders_df['order_id'] == order_id, 'shipping_cost'].iloc[0]
            total_amount = round(subtotal + tax_amount + shipping_cost, 2)
            
            # Update using .loc to avoid the warning
            orders_df.loc[orders_df['order_id'] == order_id, 'tax_amount'] = tax_amount
            orders_df.loc[orders_df['order_id'] == order_id, 'total_amount'] = total_amount
        
        return pd.DataFrame(order_items)
    
    def generate_web_events(self, customers_df, products_df, num_events=25000000):  # Increased default
        """Generate web clickstream events with batch processing"""
        print("Generating web events in batches...")
        
        events = []
        batch_size = 1000000  # Process in batches of 1M
        
        customer_ids = customers_df['customer_id'].values
        product_ids = products_df['product_id'].values
        
        event_types = ['page_view', 'add_to_cart', 'remove_from_cart', 
                      'purchase', 'search', 'login', 'logout']
        event_probs = [0.50, 0.15, 0.05, 0.08, 0.12, 0.05, 0.05]
        
        for batch_start in range(0, num_events, batch_size):
            batch_end = min(batch_start + batch_size, num_events)
            batch_size_actual = batch_end - batch_start
            
            print(f"Processing events {batch_start:,} to {batch_end:,}")
            
            # Generate batch data
            logged_in_mask = np.random.random(batch_size_actual) < 0.3
            customer_ids_batch = np.where(logged_in_mask, 
                                        np.random.choice(customer_ids, batch_size_actual),
                                        None)
            user_types = np.where(logged_in_mask, 'registered', 'anonymous')
            
            event_types_batch = np.random.choice(event_types, size=batch_size_actual, p=event_probs)
            
            # Generate product interactions
            product_interaction_mask = np.isin(event_types_batch, ['page_view', 'add_to_cart', 'remove_from_cart'])
            product_ids_batch = np.where(product_interaction_mask,
                                       np.random.choice(product_ids, batch_size_actual),
                                       None)
            
            # Generate timestamps
            timestamps = [fake.date_time_between(start_date=self.start_date, end_date=self.end_date) 
                         for _ in range(batch_size_actual)]
            
            for i in range(batch_size_actual):
                event = {
                    'event_id': batch_start + i + 1,
                    'customer_id': customer_ids_batch[i],
                    'session_id': fake.uuid4(),
                    'event_type': event_types_batch[i],
                    'product_id': product_ids_batch[i],
                    'event_timestamp': timestamps[i],
                    'user_agent': fake.user_agent(),
                    'ip_address': fake.ipv4(),
                    'referrer': fake.url() if np.random.random() < 0.6 else None,
                    'page_url': fake.url(),
                    'user_type': user_types[i],
                    'device_type': np.random.choice(['desktop', 'mobile', 'tablet'], p=[0.45, 0.45, 0.10]),
                    'country': fake.country_code(),
                    'city': fake.city()
                }
                events.append(event)
        
        return pd.DataFrame(events)
    
    def save_data(self, data_dict, output_dir='data'):
        """Save all generated data to CSV files"""
        os.makedirs(output_dir, exist_ok=True)
        
        for table_name, df in data_dict.items():
            file_path = os.path.join(output_dir, f'{table_name}.csv')
            print(f"Saving {len(df):,} records to {file_path}...")
            df.to_csv(file_path, index=False)
            print(f"✓ Saved {table_name}")
    
    def generate_all_data(self, scale_factor=1.0):
        """Generate complete ecommerce dataset with configurable scale"""
        base_customers = int(500000 * scale_factor)
        base_products = int(50000 * scale_factor)
        base_orders = int(5000000 * scale_factor)
        base_events = int(25000000 * scale_factor)
        
        print(f"Generating dataset with scale factor {scale_factor}x")
        print(f"Target: {base_customers:,} customers, {base_products:,} products, {base_orders:,} orders, {base_events:,} events")
        
        print("\nGenerating customers...")
        customers_df = self.generate_customers(base_customers)
        
        print("Generating products...")
        products_df = self.generate_products(base_products)
        
        print("Generating orders...")
        orders_df = self.generate_orders(customers_df, products_df, base_orders)
        
        print("Generating order items...")
        order_items_df = self.generate_order_items_optimized(orders_df, products_df)
        
        print("Generating web events...")
        web_events_df = self.generate_web_events(customers_df, products_df, base_events)
        
        data_dict = {
            'customers': customers_df,
            'products': products_df,
            'orders': orders_df,
            'order_items': order_items_df,
            'web_events': web_events_df
        }
        
        print("\nSaving data...")
        self.save_data(data_dict)
        
        # Print summary statistics
        print("\n=== Data Generation Summary ===")
        total_records = 0
        for table_name, df in data_dict.items():
            records = len(df)
            total_records += records
            print(f"{table_name}: {records:,} records")
        
        print(f"\nTotal records generated: {total_records:,}")
        
        return data_dict

if __name__ == "__main__":
    generator = EcommerceDataGenerator()
    
    # Generate massive dataset - adjust scale_factor as needed
    # scale_factor=1.0 = ~30M total records
    # scale_factor=2.0 = ~60M total records
    # scale_factor=0.1 = ~3M total records (for testing)
    
    data = generator.generate_all_data(scale_factor=0.1)  # Generate 2x the base amount