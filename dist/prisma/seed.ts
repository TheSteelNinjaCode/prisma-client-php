import { PrismaClient } from "@prisma/client";

type SupportedProvider = "sqlite" | "postgresql" | "mysql";

const DATABASE_URL = process.env.DATABASE_URL;

if (!DATABASE_URL) {
  throw new Error("DATABASE_URL is not set.");
}

const databaseUrl = DATABASE_URL;

function detectProvider(databaseUrl: string): SupportedProvider {
  const normalized = databaseUrl.trim().toLowerCase();

  if (normalized.startsWith("file:") || normalized.startsWith("sqlite:")) {
    return "sqlite";
  }

  if (
    normalized.startsWith("postgresql:") ||
    normalized.startsWith("postgres:")
  ) {
    return "postgresql";
  }

  if (normalized.startsWith("mysql:") || normalized.startsWith("mariadb:")) {
    return "mysql";
  }

  throw new Error(
    `Unsupported DATABASE_URL protocol in "${databaseUrl}". Supported: file:, sqlite:, postgresql:, postgres:, mysql:, mariadb:`,
  );
}

async function createPrismaClient(): Promise<{
  prisma: PrismaClient;
  provider: SupportedProvider;
  cleanup: () => Promise<void>;
}> {
  const provider = detectProvider(databaseUrl);

  if (provider === "sqlite") {
    const { PrismaBetterSqlite3 } =
      await import("@prisma/adapter-better-sqlite3");

    const adapter = new PrismaBetterSqlite3({
      url: databaseUrl,
    });

    const prisma = new PrismaClient({ adapter });

    return {
      prisma,
      provider,
      cleanup: async () => {
        await prisma.$disconnect();
      },
    };
  }

  if (provider === "postgresql") {
    const { PrismaPg } = await import("@prisma/adapter-pg");
    const adapter = new PrismaPg({
      connectionString: databaseUrl,
    });
    const prisma = new PrismaClient({ adapter });

    return {
      prisma,
      provider,
      cleanup: async () => {
        await prisma.$disconnect();
      },
    };
  }

  const { PrismaMariaDb } = await import("@prisma/adapter-mariadb");
  const adapter = new PrismaMariaDb(databaseUrl);
  const prisma = new PrismaClient({ adapter });

  return {
    prisma,
    provider,
    cleanup: async () => {
      await prisma.$disconnect();
    },
  };
}

// ============================================================
// SEED DATA
// ============================================================

const userRoleData = [
  { id: 1, name: "Admin" },
  { id: 2, name: "User" },
];

const userData = [
  {
    name: "Juan",
    email: "j@gmail.com",
    password:
      "scrypt:32768:8:1$MmggiPD6tw2gvjHr$a7adb38c13b2dcbbd72b078b65b1db046777bf6f07c4db6cd7850bba1ef39d1fce74b3cb284fcf013953cffe3e72f67651e9a4393e81855ecd36cfd16404ff7b", // temp: 123
    roleId: 1,
  },
];

// ============================================================
// RESET HELPERS
// ============================================================

async function resetSqlite(prisma: PrismaClient): Promise<void> {
  console.log("Resetting SQLite data...");

  await prisma.user.deleteMany();
  await prisma.userRole.deleteMany();

  // Optional reset of AUTOINCREMENT counters for conventional table names.
  // Safe to ignore if sqlite_sequence or table rows do not exist yet.
  try {
    await prisma.$executeRawUnsafe(
      `DELETE FROM sqlite_sequence WHERE name IN ('User', 'UserRole', 'user', 'userRole');`,
    );
  } catch {
    // ignore
  }
}

async function resetPostgreSql(prisma: PrismaClient): Promise<void> {
  console.log("Resetting PostgreSQL data...");

  await prisma.user.deleteMany();
  await prisma.userRole.deleteMany();

  // Sequence reset is intentionally skipped because actual sequence names
  // can vary depending on mapping/table naming. Seeding still works correctly.
}

async function resetMySql(prisma: PrismaClient): Promise<void> {
  console.log("Resetting MySQL / MariaDB data...");

  await prisma.user.deleteMany();
  await prisma.userRole.deleteMany();

  // AUTO_INCREMENT reset is intentionally skipped because physical table names
  // may differ if @@map is used. Seeding still works correctly.
}

// ============================================================
// MAIN SEED
// ============================================================

async function main(): Promise<void> {
  const { prisma, provider, cleanup } = await createPrismaClient();

  try {
    console.log(`Start seeding...`);
    console.log(`Detected provider: ${provider}`);
    console.log(`DATABASE_URL: ${databaseUrl}`);

    if (provider === "sqlite") {
      await resetSqlite(prisma);
    } else if (provider === "postgresql") {
      await resetPostgreSql(prisma);
    } else {
      await resetMySql(prisma);
    }

    await prisma.userRole.createMany({
      data: userRoleData,
    });

    await prisma.user.createMany({
      data: userData,
    });

    console.log("Seeding finished successfully.");
  } catch (error) {
    console.error("Seed failed:", error);
    process.exitCode = 1;
  } finally {
    await cleanup();
  }
}

void main();
