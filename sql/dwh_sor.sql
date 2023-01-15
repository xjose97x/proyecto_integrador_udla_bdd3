USE pronacasor;

CREATE TABLE COMPRA_PRODUCTOS (
    ID INTEGER NOT NULL AUTO_INCREMENT,
    ITEM VARCHAR(150) NOT NULL,
    MONTH INTEGER NOT NULL,
    YEAR INTEGER NOT NULL,
    VALUE DECIMAL(10,2) NOT NULL,
    PRIMARY KEY (ID)
);

CREATE TABLE COMPRAS_CONSOLIDADAS (
    ID INTEGER NOT NULL AUTO_INCREMENT,
    CATEGORIA VARCHAR(30) NOT NULL,
    TOTAL_USD DECIMAL(10,2) NOT NULL,
    TOTAL_PORCENTAJE DECIMAL(10,2) NOT NULL,
    PRECIO_USD DECIMAL(10,2) NOT NULL,
    PRECIO_PORCENTAJE DECIMAL(10,2) NOT NULL,
    VOLUMEN_USD DECIMAL(10,2) NOT NULL,
    VOLUMEN_PORCENTAJE DECIMAL(10,2) NOT NULL,
    PRIMARY KEY (ID)
);

CREATE TABLE INGRESOS (
    ID INTEGER NOT NULL AUTO_INCREMENT,
    YEAR VARCHAR(30) NOT NULL,
    MONTH VARCHAR(30) NOT NULL,
    VALUE DECIMAL(10,2) NOT NULL,
    PRIMARY KEY (ID)
);

CREATE TABLE GASTOS (
    ID INTEGER NOT NULL AUTO_INCREMENT,
    YEAR INTEGER NOT NULL,
    MONTH INTEGER NOT NULL,
    VALUE DECIMAL(10,2) NOT NULL,
    PRIMARY KEY (ID)
);

CREATE TABLE RIESGO_PROVEEDORES (
    ID INTEGER NOT NULL AUTO_INCREMENT,
    PROVEEDOR VARCHAR(150) NOT NULL,
    YEAR INTEGER NOT NULL,
    RIESGO DECIMAL(10,2) NOT NULL,
    PRIMARY KEY (ID)
);