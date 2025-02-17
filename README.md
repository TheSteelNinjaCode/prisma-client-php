# Prisma Client PHP

**Prisma Client PHP** is a standalone package that brings Prisma ORM functionality to PHP, allowing seamless interaction with a Prisma-managed database. It enables developers to work with database models using an elegant, type-safe, and class-based approach, similar to how Prisma works with TypeScript.

## Introduction

Prisma Client PHP is a key component of the **`create-prisma-php-app`** package, which provides a fast and efficient PHP environment for development. It features component-based architecture and file-based routing, similar to Next.js and React, but optimized for PHP.

This package is responsible for integrating and managing the Prisma ORM layer in PHP. It generates convenient PHP classes based on your database models, replicating the behavior of `npx prisma generate` in TypeScript but tailored for PHP. This allows developers to effortlessly access, modify, and manage database records using a structured and intuitive API.

## Prerequisites

Ensure that your system meets the following requirements before installing Prisma Client PHP:

- **Node.js**: Version 18.x or higher - [Download Node.js](https://nodejs.org/en/download/)
- **XAMPP** (or any PHP 8.2+ environment) - [Download XAMPP](https://www.apachefriends.org/download.html)
- **Composer**: Version 2.x or higher - [Download Composer](https://getcomposer.org/download/)
- **NPM Package Manager**: For more details and installation instructions, visit the [create-prisma-php-app](https://www.npmjs.com/package/create-prisma-php-app) page on npm.

## Installation

Follow these steps to install and set up Prisma Client PHP:

1. **Open your terminal**.
2. **Generate a new Prisma PHP project** in your desired directory by running:

   ```bash
   npx create-prisma-php-app@latest
   ```

3. **Generate Prisma Client PHP classes** by running:

   ```bash
   npx ppo generate
   ```

This command will convert your `schema.prisma` models into PHP classes, enabling you to interact with your database using these generated classes.

## Features

- **Auto-Generated PHP Classes**: Prisma Client PHP automatically generates PHP classes based on your Prisma schema.
- **Type-Safe Database Queries**: Work with your database in a structured and predictable way.
- **Seamless ORM Integration**: Leverages Prisma ORM’s powerful query engine within a PHP environment.
- **Component-Based Development**: Works alongside `create-prisma-php-app`, providing a modern development experience.
- **File-Based Routing**: Inspired by Next.js and React, simplifying route management in PHP projects.

## Contributing

We welcome contributions to Prisma Client PHP! If you have suggestions, bug reports, or pull requests, feel free to open an issue or submit a PR in the repository.

## License

Prisma Client PHP is licensed under the **MIT License**. See the `LICENSE` file for more details.

## Author

Prisma Client PHP is developed and maintained by **[The Steel Ninja Code](https://thesteelninjacode.com/).**

## Contact Us

For support, feedback, or inquiries, reach out to us at **[thesteelninjacode@gmail.com](mailto:thesteelninjacode@gmail.com).**
