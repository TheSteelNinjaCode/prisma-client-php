import { PrismaClient } from "@prisma/client";

// ============================================================
// 1. DATABASE ADAPTER CONFIGURATION
// Uncomment the section matching your active database
// ============================================================

// --- OPTION A: SQLITE ---------------------------------------
// import { PrismaBetterSqlite3 } from "@prisma/adapter-better-sqlite3";

// const adapter = new PrismaBetterSqlite3({
//   url: process.env.DATABASE_URL!,
// });
// const prisma = new PrismaClient({ adapter });
// ------------------------------------------------------------

// --- OPTION B: POSTGRESQL -----------------------------------
// import { PrismaPg } from "@prisma/adapter-pg";
// import { Pool } from "pg";

// const connectionString = process.env.DATABASE_URL!;
// const pool = new Pool({ connectionString });
// const adapter = new PrismaPg(pool);
// const prisma = new PrismaClient({ adapter });
// ------------------------------------------------------------

// --- OPTION C: MYSQL / MARIADB ------------------------------
// import { PrismaMariaDb } from "@prisma/adapter-mariadb";
// import { createPool } from "mariadb";

// const connectionUrl = new URL(process.env.DATABASE_URL!);
// const pool = createPool({
//   host: connectionUrl.hostname,
//   port: Number(connectionUrl.port) || 3306,
//   user: connectionUrl.username,
//   password: connectionUrl.password,
//   database: connectionUrl.pathname.slice(1),
//   connectionLimit: 5,
// });
// const adapter = new PrismaMariaDb(pool);
// const prisma = new PrismaClient({ adapter });
// ------------------------------------------------------------

// --- DEFAULT (NO ADAPTER) -----------------------------------
// If you just want standard Prisma without specific drivers:
const prisma = new PrismaClient();
// ------------------------------------------------------------

// ============================================================
// 2. DATA DEFINITION
// ============================================================

const userRoleData = [
  { id: 1, name: "Admin" },
  { id: 2, name: "User" },
];

const userData = [
  {
    name: "Juan",
    email: "j@gmail.com",
    password: "$2b$10$mgjotYzIXwrK1MCWmu4tgeUVnLcb.qzvqwxOq4FXEL8k2obwXivDi", // bcrypt: 1234
    roleId: 1,
  },
];

// ============================================================
// 3. EXECUTION LOGIC
// ============================================================

async function main() {
  console.log(`Start seeding ...`);

  // NOTE: We delete User first, then UserRole to avoid Foreign Key constraints.

  // --- OPTION A: SQLITE LOGIC ---------------------------------
  /*
  console.log("Seeding for SQLite...");
  
  await prisma.user.deleteMany();
  await prisma.userRole.deleteMany();
  // SQLite implies auto-increment reset via internal table if needed, 
  // but usually not strictly required for seeds unless using specific IDs.
  
  await prisma.userRole.createMany({ data: userRoleData });
  await prisma.user.createMany({ data: userData });
  */
  // ------------------------------------------------------------

  // --- OPTION B: POSTGRESQL LOGIC -----------------------------
  /*
  console.log("Seeding for PostgreSQL...");

  await prisma.user.deleteMany();
  await prisma.userRole.deleteMany();
  
  // Reset Auto Increment Sequences
  await prisma.$executeRaw`ALTER SEQUENCE "UserRole_id_seq" RESTART WITH 1`;
  await prisma.$executeRaw`ALTER SEQUENCE "User_id_seq" RESTART WITH 1`;

  await prisma.userRole.createMany({ data: userRoleData });
  await prisma.user.createMany({ data: userData });
  */
  // ------------------------------------------------------------

  // --- OPTION C: MYSQL LOGIC ----------------------------------
  /*
  console.log("Seeding for MySQL...");

  await prisma.user.deleteMany();
  await prisma.userRole.deleteMany();

  // Reset Auto Increment Counters
  await prisma.$executeRaw`ALTER TABLE UserRole AUTO_INCREMENT = 1`;
  await prisma.$executeRaw`ALTER TABLE User AUTO_INCREMENT = 1`;

  await prisma.userRole.createMany({ data: userRoleData });
  await prisma.user.createMany({ data: userData });
  */
  // ------------------------------------------------------------

  console.log(`Seeding finished.`);
}

main()
  .catch((e) => {
    console.error(e);
    process.exit(1);
  })
  .finally(async () => {
    await prisma.$disconnect();
  });
