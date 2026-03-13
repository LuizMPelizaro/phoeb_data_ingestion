package com.phoeb_data_ingestion.domain.silver

import java.sql.Timestamp

case class EnaCarga(
                     id_subsistema: String,
                     nom_subsistema: String,
                     ena_data: Timestamp,
                     ena_bruta_regiao_mwmed: Double,
                     ena_bruta_regiao_percentualmlt: Double,
                     ena_armazenavel_regiao_mwmed: Double,
                     ena_armazenavel_regiao_percentualmlt: Double
                   )