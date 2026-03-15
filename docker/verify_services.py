import socket
import sys

def check_port(host, port, service_name):
    """Try to connect to a host:port to verify a service is running."""
    try:
        sock = socket.create_connection((host, port), timeout=5)
        sock.close()
        print(f"  ✅ {service_name} is reachable at {host}:{port}")
        return True
    except (socket.timeout, ConnectionRefusedError, OSError):
        print(f"  ❌ {service_name} is NOT reachable at {host}:{port}")
        return False

print("🔍 Checking running services...\n")

results = [
    check_port("localhost", 2181, "Zookeeper"),
    check_port("localhost", 9092, "Kafka"),
    check_port("localhost", 5432, "PostgreSQL"),
]

print()
if all(results):
    print("🎉 All services are running! Ready for Phase 4.")
else:
    print("⚠️  Some services are not running. Check docker-compose logs.")
    sys.exit(1)