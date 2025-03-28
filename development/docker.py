import docker
import time

def start_postgres_container():
    client = docker.from_env()
    container = client.containers.run(
        "postgres:latest",
        detach=True,
        environment={
            "POSTGRES_USER": "fuzzuser",
            "POSTGRES_PASSWORD": "fuzzpass",
            "POSTGRES_DB": "fuzzdb"
        },
        ports={'5432/tcp': 5432},
        name="pg_fuzzer"
    )
    
    # Wait for DB to initialize
    time.sleep(10)
    return container

def stop_postgres_container():
    client = docker.from_env()
    container = client.containers.get("pg_fuzzer")
    container.stop()
    container.remove()

# Test this phase
print("Starting PostgreSQL container...")
container = start_postgres_container()
print(f"Container ID: {container.id}")
input("Press Enter to stop and remove container...")
stop_postgres_container()
print("Container cleaned up!")