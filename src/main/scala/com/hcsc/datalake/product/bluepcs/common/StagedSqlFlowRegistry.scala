package com.hcsc.datalake.product.bluepcs.common

import com.hcsc.datalake.product.bluepcs.core.BluepcssPMMPlusLoader.{SqlStage, StagedSqlFlow}

object StagedSqlFlowRegistry {

  val stagedSqlFlows: Map[String, StagedSqlFlow] = Map(
    // Add staged SQL flows here as needed
    // Example:
    // "benefittiers" -> StagedSqlFlow(
    //   stages = Seq(
    //     SqlStage("benefittiers_stage1_sql", "benefittiers_stage1_view"),
    //     SqlStage("benefittiers_stage2_sql", "benefittiers_stage2_view")
    //   ),
    //   finalSqlFile = "benefittiers_final_sql"
    // )
  )
}
