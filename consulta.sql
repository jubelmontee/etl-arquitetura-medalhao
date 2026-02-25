select 
users.nome,
users.email,
users.telefone,
users.data_nascimento,
users.cep,
cep_info.logradouro,
cep_info.bairro,
cep_info.estado,
cep_info.localidade
from users
INNER JOIN cep_info ON users.cep = cep_info.cep
order by cast(users.id as int) asc;