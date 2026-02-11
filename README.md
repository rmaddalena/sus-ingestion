# sus_ingestion

Exemplo de ingestão de dados do sus feito como resolução de um case.
Todo o pipeline foi construído usando o Databricks Free Edition. Os arquivos do sus foram extraídos do site https://datasus.saude.gov.br/transferencia-de-arquivos/# manualmente, e depois inseridos nos 'volumes' do Databricks 

Descrição das pastas:
- `Transformations`: É aqui onde o Databricks procura os arquivos que vão gerar as tabelas do pipeline. Estão seprados em camadas bronze, prata, ouro e semântica, de acordo com a teoria da arquitetura medalhão
- `assertions`: São notebooks python focados em testar a qualidade dos dados
- `explorations`: Contém exemplos de notebooks python para exploração dos dados
- `utilities`: Funções úteis e notebooks para transformação de dados na camada raw
