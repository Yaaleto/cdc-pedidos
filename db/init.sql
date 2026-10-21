CREATE TABLE pedidos (
    id             SERIAL PRIMARY KEY,
    cliente        VARCHAR(100)  NOT NULL,
    producto       VARCHAR(100)  NOT NULL,
    cantidad       INT           NOT NULL CHECK (cantidad > 0),
    total          NUMERIC(10,2) NOT NULL,
    estado         VARCHAR(20)   NOT NULL DEFAULT 'pendiente',
    creado_en      TIMESTAMPTZ   NOT NULL DEFAULT now(),
    actualizado_en TIMESTAMPTZ   NOT NULL DEFAULT now()
);

-- Para que UPDATE y DELETE incluyan el estado anterior completo (campo "before")
ALTER TABLE pedidos REPLICA IDENTITY FULL;