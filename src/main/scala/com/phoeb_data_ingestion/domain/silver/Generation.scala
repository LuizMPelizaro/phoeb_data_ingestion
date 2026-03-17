package com.phoeb_data_ingestion.domain.silver

import java.sql.Timestamp

case class Generation(
                       din_instante: Timestamp,
                       id_subsistema: String,
                       nom_subsistema: String,
                       id_estado: String,
                       nom_estado: String,
                       cod_modalidadeoperacao: String,
                       nom_tipousina: String,
                       nom_tipocombustivel: String,
                       nom_usina: String,
                       id_ons: String,
                       ceg: String,
                       val_geracao: Double
                     )
