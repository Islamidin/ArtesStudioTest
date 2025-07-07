## Running the Project with Docker

This project is composed of two main .NET services (`MatchMaking.Service` and `MatchMaking.Worker`) and uses Kafka and Zookeeper as dependencies. The setup is orchestrated via Docker Compose.

### Project-Specific Docker Requirements
- **.NET Version:** Both services require .NET 9.0 (as specified by `ARG DOTNET_VERSION=9.0` in the Dockerfiles).
- **Kafka & Zookeeper:** The services depend on Kafka (Bitnami image) and Zookeeper (Bitnami image) containers.
- **Non-root User:** Both service containers run as a non-root user for improved security.

### Environment Variables
- No required environment variables are specified in the Dockerfiles or compose file for the application services by default.
- Kafka and Zookeeper containers use the following environment variables (set in `docker-compose.yml`):
  - **Kafka:**
    - `KAFKA_BROKER_ID=1`
    - `KAFKA_LISTENERS=PLAINTEXT://:9092`
    - `KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka:9092`
    - `KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181`
    - `ALLOW_PLAINTEXT_LISTENER=yes`
  - **Zookeeper:**
    - `ALLOW_ANONYMOUS_LOGIN=yes`
- If you need to add application-specific environment variables, uncomment and use the `env_file` lines in the compose file for each service.

### Build and Run Instructions
1. **Ensure Docker and Docker Compose are installed.**
2. **From the project root directory (where `docker-compose.yml` is located), run:**
   ```sh
   docker compose up --build
   ```
   This will build and start all services: `csharp-matchmaking-service`, `csharp-matchmaking-worker`, `kafka`, and `zookeeper`.

### Ports Exposed
- **csharp-matchmaking-service:**
  - Exposes port **80** inside the container, mapped to **8080** on the host (`8080:80`).
  - Access the API at `http://localhost:8080/`.
- **Kafka and Zookeeper:**
  - Internal networking only; no ports are published to the host by default.

### Special Configuration
- The services are connected via a custom Docker network called `matchmaking`.
- Both services depend on Kafka being available before starting.
- Health checks are configured for Kafka and Zookeeper containers.
- If you need to customize application settings, edit the `appsettings.json` or `appsettings.Development.json` files in each service directory.

---

*This section is up to date with the current Docker setup. If you add new services or change ports, update this section accordingly.*
