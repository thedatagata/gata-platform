FROM denoland/deno:2.5.6

# Set working directory
WORKDIR /app

# Copy dependency files first for better caching
COPY deno.json deno.lock* ./

# Copy the rest of the application
COPY . .

# Cache the dependencies and compile main.ts
RUN deno install --entrypoint main.ts

# Create db directory with proper permissions
RUN mkdir -p /app/db && chmod 755 /app/db

# Build the application
ENV BUILD_PHASE=true
RUN deno task build
ENV BUILD_PHASE=

# Expose the port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD deno eval "fetch('http://localhost:8000/health').then(r => r.ok ? Deno.exit(0) : Deno.exit(1))"

# Run the application
CMD ["deno", "run", "-A", "--unstable-kv", "main.ts"]
