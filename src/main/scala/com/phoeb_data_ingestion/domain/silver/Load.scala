package com.phoeb_data_ingestion.domain.silver

import java.sql.Timestamp

case class Load(
                 id_subsistema: String,
                 nom_subsistema: String,
                 din_instante: Timestamp,
                 val_cargaenergiahomwmed: Double
               )
