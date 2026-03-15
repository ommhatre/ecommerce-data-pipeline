import uuid
import random
import json
from datetime import datetime
from faker import Faker

fake = Faker()

# ─────────────────────────────────────────
# PRODUCT CATALOGUE
# Our fake store's inventory
# ─────────────────────────────────────────
PRODUCT_CATALOGUE = [
    {"product_id": 1,  "product_name": "Laptop Pro 15",         "category": "electronics", "price": 1299.99},
    {"product_id": 2,  "product_name": "Wireless Headphones",   "category": "electronics", "price": 149.99},
    {"product_id": 3,  "product_name": "Smartphone X12",        "category": "electronics", "price": 899.99},
    {"product_id": 4,  "product_name": "USB-C Hub 7-in-1",      "category": "electronics", "price": 49.99},
    {"product_id": 5,  "product_name": "Webcam HD 1080p",       "category": "electronics", "price": 79.99},
    {"product_id": 6,  "product_name": "Running Shoes",         "category": "clothing",    "price": 89.99},
    {"product_id": 7,  "product_name": "Denim Jacket",          "category": "clothing",    "price": 69.99},
    {"product_id": 8,  "product_name": "Sports Socks 6-Pack",   "category": "clothing",    "price": 14.99},
    {"product_id": 9,  "product_name": "Winter Coat",           "category": "clothing",    "price": 199.99},
    {"product_id": 10, "product_name": "Baseball Cap",          "category": "clothing",    "price": 24.99},
    {"product_id": 11, "product_name": "Python Crash Course",   "category": "books",       "price": 39.99},
    {"product_id": 12, "product_name": "Data Warehouse Toolkit","category": "books",       "price": 59.99},
    {"product_id": 13, "product_name": "Clean Code",            "category": "books",       "price": 34.99},
    {"product_id": 14, "product_name": "Atomic Habits",         "category": "books",       "price": 27.99},
    {"product_id": 15, "product_name": "The Pragmatic Programmer","category": "books",     "price": 49.99},
    {"product_id": 16, "product_name": "Coffee Maker Deluxe",   "category": "home",        "price": 89.99},
    {"product_id": 17, "product_name": "Air Purifier Pro",      "category": "home",        "price": 129.99},
    {"product_id": 18, "product_name": "LED Desk Lamp",         "category": "home",        "price": 39.99},
    {"product_id": 19, "product_name": "Throw Blanket",         "category": "home",        "price": 44.99},
    {"product_id": 20, "product_name": "Smart Plug 4-Pack",     "category": "home",        "price": 29.99},
    {"product_id": 21, "product_name": "Yoga Mat Premium",      "category": "sports",      "price": 59.99},
    {"product_id": 22, "product_name": "Dumbbells Set 20kg",    "category": "sports",      "price": 79.99},
    {"product_id": 23, "product_name": "Resistance Bands Kit",  "category": "sports",      "price": 19.99},
    {"product_id": 24, "product_name": "Jump Rope Pro",         "category": "sports",      "price": 14.99},
    {"product_id": 25, "product_name": "Water Bottle 1L",       "category": "sports",      "price": 24.99},
]

# ─────────────────────────────────────────
# EVENT TYPE WEIGHTS
# Users view products most often,
# add to cart sometimes, purchase rarely.
# This mirrors real e-commerce behaviour.
# ─────────────────────────────────────────
EVENT_TYPES = ["page_view", "add_to_cart", "purchase"]
EVENT_WEIGHTS = [70, 20, 10]  # 70% views, 20% cart, 10% purchases

# Simulated pool of users (user_ids 1 to 500)
USER_POOL_SIZE = 500


def generate_event() -> dict:
    """
    Generate a single realistic e-commerce event.

    Returns a dictionary representing one user action.
    """
    # Pick a random product from our catalogue
    product = random.choice(PRODUCT_CATALOGUE)

    # Pick an event type — weighted so views are most common
    event_type = random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS, k=1)[0]

    # Purchases can have quantity > 1, others are always 1
    quantity = random.randint(1, 3) if event_type == "purchase" else 1

    event = {
        "event_id":     str(uuid.uuid4()),
        "user_id":      random.randint(1, USER_POOL_SIZE),
        "event_type":   event_type,
        "product_id":   product["product_id"],
        "product_name": product["product_name"],
        "category":     product["category"],
        "price":        product["price"],
        "quantity":     quantity,
        "revenue":      round(product["price"] * quantity, 2),
        "timestamp":    datetime.utcnow().isoformat(),
    }

    return event


def generate_batch(batch_size: int = 10) -> list:
    """
    Generate multiple events at once.

    Args:
        batch_size: How many events to generate

    Returns:
        A list of event dictionaries
    """
    return [generate_event() for _ in range(batch_size)]


def format_event_for_display(event: dict) -> str:
    """
    Format an event as a human-readable string for terminal output.
    """
    icons = {
        "page_view":    "👁️ ",
        "add_to_cart":  "🛒",
        "purchase":     "💰",
    }
    icon = icons.get(event["event_type"], "❓")

    return (
        f"{icon}  [{event['event_type'].upper():12}] "
        f"User {event['user_id']:4} | "
        f"{event['product_name']:30} | "
        f"${event['price']:7.2f} | "
        f"{event['category']}"
    )


# ─────────────────────────────────────────
# MAIN — Run this file directly to test
# ─────────────────────────────────────────
if __name__ == "__main__":
    import time

    print("🚀 E-Commerce Event Generator Started")
    print("=" * 70)
    print("Generating live events... (Press CTRL+C to stop)\n")

    total_events = 0
    event_counts = {"page_view": 0, "add_to_cart": 0, "purchase": 0}

    try:
        while True:
            event = generate_event()
            event_counts[event["event_type"]] += 1
            total_events += 1

            # Print formatted event to terminal
            print(format_event_for_display(event))

            # Every 20 events, print a summary
            if total_events % 20 == 0:
                print("\n" + "─" * 70)
                print(f"📊 SUMMARY — Total events: {total_events}")
                print(f"   👁️  Page views:    {event_counts['page_view']}")
                print(f"   🛒 Cart additions: {event_counts['add_to_cart']}")
                print(f"   💰 Purchases:      {event_counts['purchase']}")
                print("─" * 70 + "\n")

            # Generate one event per second
            time.sleep(1)

    except KeyboardInterrupt:
        print(f"\n\n✅ Generator stopped. Total events generated: {total_events}")