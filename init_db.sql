CREATE DATABASE IF NOT EXISTS sellout_db;

USE sellout_db;

CREATE TABLE IF NOT EXISTS sellout (
    DISTRIBUTOR_CODE INT,
    SELLOUT_DATE DATE,
    CUSTOMER_ID VARCHAR(50),
    SELLOUT_TYPE VARCHAR(5),
    INVOICE_ID VARCHAR(50),
    PRODUCT_CODE VARCHAR(50),
    SALESREP_ID VARCHAR(50),
    QTY_UNIT FLOAT,
    QTY_CONV1 FLOAT,
    QTY_CONV2 FLOAT,
    QTY_CONV3 FLOAT,
    QTY_CONV4 FLOAT,
    QTY_CONV5 FLOAT,
    QTY_CONV6 FLOAT,
    QTY_CONV7 FLOAT,
    QTY_CUSTOM1 FLOAT,
    QTY_CUSTOM2 FLOAT,
    SELLOUT_VALUE_LC FLOAT,
    SELLOUT_CONV1 FLOAT
);


CREATE TABLE IF NOT EXISTS produtos (
    PRODUCT_CODE INT NOT NULL,
    PRODUCT_EAN_DUN_ID VARCHAR(50),
    PRODUCT_SKU_CODE VARCHAR(50),
    PRODUCT_NAME VARCHAR(255),
    PRIMARY KEY (PRODUCT_CODE)
);


CREATE TABLE  IF NOT EXISTS distribuidores (
    DISTRIBUTOR_CODE INT NOT NULL,
    DISTRIBUTOR_ID VARCHAR(50),
    DISTRIBUTOR_NAME VARCHAR(255),
    DISTRIBUTOR_GROUP_NAME VARCHAR(255),
    DISTRIBUTOR_FLAG VARCHAR(1),
    DISTRIBUTOR_CHANNEL VARCHAR(50),
    SF_LEVEL1 VARCHAR(255),
    SF_LEVEL2 VARCHAR(255),
    SF_LEVEL3 VARCHAR(255),
    SF_LEVEL4 VARCHAR(5),
    SF_LEVEL5 VARCHAR(5),
    PRIMARY KEY (DISTRIBUTOR_CODE)
);
CREATE TABLE IF NOT EXISTS estoque (
    DISTRIBUTOR_CODE INT NOT NULL,
    PRODUCT_CODE VARCHAR(50) NOT NULL,
    STOCK_DATE DATE NOT NULL,
    QTY_UNIT FLOAT,
    QTY_CONV1 FLOAT,
    QTY_CONV2 FLOAT,
    QTY_CONV3 FLOAT,
    QTY_CONV4 FLOAT,
    QTY_CONV5 FLOAT,
    QTY_CONV6 FLOAT,
    QTY_CONV7 FLOAT,
    QTY_CUSTOM1 FLOAT,
    QTY_CUSTOM2 FLOAT,
    PRIMARY KEY (DISTRIBUTOR_CODE, PRODUCT_CODE, STOCK_DATE)
);

CREATE TABLE IF NOT EXISTS forca_vendas (
    DISTRIBUTOR_CODE INT NOT NULL,
    SF_YEAR_MONTH VARCHAR(6) NOT NULL,
    SALESREP_ID VARCHAR(50) NOT NULL,
    SF_LEVEL2_ID VARCHAR(50),
    SF_LEVEL1_ID VARCHAR(50),
    PRIMARY KEY (DISTRIBUTOR_CODE, SF_YEAR_MONTH, SALESREP_ID)
);