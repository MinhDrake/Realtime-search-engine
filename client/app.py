import time
import json
import threading
from elasticsearch import Elasticsearch
from kafka import KafkaProducer
from rich.console import Console
from rich.table import Table
from rich.prompt import Prompt
from rich.panel import Panel

# Config
ES_HOST = "http://localhost:9200"
KAFKA_BOOTSTRAP = "localhost:9092"
SEARCH_TOPIC = "search.queries"
TRENDING_INDEX = "trending"
PRODUCTS_INDEX = "dbserver1.public.products"

console = Console()
es = Elasticsearch(ES_HOST)
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def search_products(query):
    """
    1. Send search query to Kafka (for Analytics)
    2. Search Elasticsearch (for Results)
    """
    # 1. Log event to Kafka
    event = {"keyword": query.lower().strip(), "ts": int(time.time() * 1000)}
    producer.send(SEARCH_TOPIC, event)
    producer.flush()

    # 2. Search Elastic
    resp = es.search(
        index=PRODUCTS_INDEX,
        body={
            "query": {
                "multi_match": {
                    "query": query,
                    "fields": ["name", "description", "category"],
                    "fuzziness": "AUTO"
                }
            }
        }
    )
    return resp['hits']['hits']

def get_trending():
    """Fetch trending keywords from the 'trending' index (populated by KSQL->Connect->ES)"""
    try:
        # We want the most recent window for each keyword
        resp = es.search(
            index=TRENDING_INDEX,
            body={
                "size": 5,
                "sort": [{"WINDOW_START": "desc"}, {"COUNT": "desc"}]
            }
        )
        return resp['hits']['hits']
    except Exception:
        return []

def display_results(hits, trending):
    console.clear()
    
    # Show Trending
    console.print(Panel("🔥 [bold red]Real-time Trending Keywords (Last 60s)[/bold red]"))
    if trending:
        t_str = ", ".join([f"[bold]{h['_source']['KEYWORD']}[/bold] ({h['_source']['COUNT']})" for h in trending])
        console.print(t_str + "\n")
    else:
        console.print("[italic]No trending data yet... start searching![/italic]\n")

    # Show Results
    table = Table(title="Search Results")
    table.add_column("Name", style="cyan")
    table.add_column("Category", style="magenta")
    table.add_column("Price", justify="right")
    
    for hit in hits:
        src = hit['_source']
        table.add_row(src['name'], src['category'], f"${src['price']}")
    
    console.print(table)

def main():
    console.print("[bold green]Welcome to Realtime Search Engine![/bold green]")
    
    while True:
        query = Prompt.ask("\n🔍 [bold]Search for a product[/bold] (or 'quit')")
        if query.lower() == 'quit':
            break
        
        # Perform Search
        hits = search_products(query)
        
        # Fetch trending (simulated async, usually this updates in background)
        time.sleep(0.5) # Give slight delay for Kafka->KSQL loop if running locally
        trending = get_trending()
        
        display_results(hits, trending)

if __name__ == "__main__":
    main()
