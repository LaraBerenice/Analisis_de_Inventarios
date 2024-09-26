
CREATE DATABASE "XXX"

-- Posicionamiento en la BD ---------

USE XXX

---------- este codigo es para configurar el acceso de todos a la base de datos-------------------

-- Cambiar al contexto de la base de datos master
USE [master];

-- Crear la base de datos
CREATE DATABASE [XXX];  -- Cambia XXX por el nombre de tu base de datos

-- Cambiar al contexto de la base de datos reci�n creada
USE [XXX];

-- Crear el inicio de sesi�n para el administrador (t�)
CREATE LOGIN [LARA] WITH PASSWORD = 'TuContrase�aSegura';  -- Cambia por una contrase�a segura

-- Crear usuario en la base de datos para el administrador
CREATE USER [LARA] FOR LOGIN [LARA];
EXEC sp_addrolemember 'db_owner', 'LARA';  -- Asignar permisos de administrador

-- Crear inicios de sesi�n para los otros usuarios (cambia los nombres y contrase�as seg�n corresponda)
CREATE LOGIN [usuario1] WITH PASSWORD = 'Contrase�aSegura1';  -- Cambia usuario1 y su contrase�a
CREATE LOGIN [usuario2] WITH PASSWORD = 'Contrase�aSegura2';  -- Cambia usuario2 y su contrase�a
CREATE LOGIN [usuario3] WITH PASSWORD = 'Contrase�aSegura3';  -- Cambia usuario3 y su contrase�a

-- Cambiar al contexto de tu base de datos
USE [XXX];

-- Crear usuarios en la base de datos para los compa�eros
CREATE USER [usuario1] FOR LOGIN [usuario1];
CREATE USER [usuario2] FOR LOGIN [usuario2];
CREATE USER [usuario3] FOR LOGIN [usuario3];

-- Asignar permisos de administrador a todos los usuarios
EXEC sp_addrolemember 'db_owner', 'usuario1';  
EXEC sp_addrolemember 'db_owner', 'usuario2';  
EXEC sp_addrolemember 'db_owner', 'usuario3';
