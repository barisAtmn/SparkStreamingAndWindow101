package com.baris.spark101
import org.apache.spark.sql.SparkSession

/**
  * You are able to enhance spark functionality.
  * Delta part is injected from here.
  * spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension
  * be careful with extensions, you cant make spark worst!!!
 **/
object SparkSessionExtensionsT {
  val spark = SparkSession.builder().appName("Extensions").withExtensions {
    ???
  }
}
/**
  * class DeltaSparkSessionExtension extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectParser { (session, parser) =>
      new DeltaSqlParser(parser)
    }
    extensions.injectResolutionRule { session =>
      new DeltaAnalysis(session, session.sessionState.conf)
    }
    extensions.injectCheckRule { session =>
      new DeltaUnsupportedOperationsCheck(session)
    }
    extensions.injectPostHocResolutionRule { session =>
      new PreprocessTableUpdate(session.sessionState.conf)
    }
    extensions.injectPostHocResolutionRule { session =>
      new PreprocessTableMerge(session.sessionState.conf)
    }
    extensions.injectPostHocResolutionRule { session =>
      new PreprocessTableDelete(session.sessionState.conf)
    }
    extensions.injectOptimizerRule { session =>
      new ActiveOptimisticTransactionRule(session)
    }
  }
}
 **/
