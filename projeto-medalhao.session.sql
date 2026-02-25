SELECT
    u.nome,
    (EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM TO_DATE(u.data_nascimento, 'YYYY-MM-DD'))) AS idade,
    u.genero,
    u.cep,
    c.logradouro,
    c.bairro,
    c.localidade,
    c.estado
FROM users u
INNER JOIN cep_info c ON u.cep = c.cep
ORDER BY u.nome asc