  package com.kh.withgraphx

  import org.apache.log4j.{Level, Logger}
  import org.apache.spark.SparkContext
  import org.apache.spark.graphx._

  object AutomataMinimizationPregel {

    sealed trait CustomBoolean
    case object _True extends CustomBoolean
    case object _False extends CustomBoolean
    case object _Null extends CustomBoolean


    case class BlockId(value: String, size: Int)
    case class StateData(blockId: BlockId = null, vote: CustomBoolean = _Null, stopSignal: CustomBoolean = _Null)

    case class Message( blockIdCollection: List[(String, String)] = List(),
                        votes: CustomBoolean = _Null,
                        stopSignal: CustomBoolean = _Null )

    val initialMessage: Message = Message()

    val alphabetSize: Int = 2
    val supervisorId: Int = -1


  def vProg(vertexId: VertexId, stateData: StateData, message: Message): StateData = {

    if(vertexId == supervisorId) { // si le sommet est le superviseur
      if(stateData.stopSignal == _Null) return StateData(stopSignal = _False)
      else {
        if (stateData.stopSignal == _True) return StateData(stopSignal = _False)
        else if (message.votes == _True) {
          return StateData(stopSignal = _True)
        }
        else return stateData
      }
    }
    else { // si le sommet est un etat
      if(message.stopSignal != _True){
        if(stateData.vote == _Null) return StateData(stateData.blockId, _False)
        else {
          val in_blockIdsCollection = message.blockIdCollection
            .sortWith{case ((_, alpha1), (_, alpha2)) => alpha1.compareTo(alpha2) < 0}
            .map{case (blockId, _) => blockId}
          val in_blockIdsSting = in_blockIdsCollection.reduceLeft(_+_)
          val new_stateBlockId = stateData.blockId.value + in_blockIdsSting

          val new_blocksSize = (stateData.blockId.value :: in_blockIdsCollection).toSet.size
          var vote: CustomBoolean = _Null
          if (stateData.blockId.size != new_blocksSize) vote = _False
          else vote = _True

          return StateData(BlockId(new_stateBlockId, new_blocksSize), vote)

        }
      } else return StateData(stateData.blockId, _Null)
    }
  }

  def sendMsg(triplet: EdgeTriplet[StateData, String]): Iterator[(VertexId, Message)] = {

    if(triplet.dstId == supervisorId) { // si le sommet envoyeur est le superviseur
      if(triplet.dstAttr.stopSignal == _True) {
        val message = (triplet.srcId, Message(stopSignal = _True))
        Iterator(message)
      }
      else return Iterator.empty
    }else { // si le sommet envoyeur est un etat
      if(triplet.dstAttr.vote != _Null){
        if(triplet.srcId == supervisorId) { // si le destinataire est le superviseur
          Iterator((supervisorId, Message(votes = triplet.dstAttr.vote)))
        }
        else{// si l'envoyeur et le destinataire sont tous des etats
          val blockId_alpha = List((triplet.dstAttr.blockId.value, triplet.attr))
          Iterator((triplet.srcId, Message(blockIdCollection = blockId_alpha)))
        }
      } else return Iterator.empty
    }

  }

  def mergeMsg(message1: Message, message2: Message): Message = {
    // si le receveur est le superviseur
    if(message1.votes != _Null && message1.votes != null) {
      val votes = if(message1.votes == _False || message2.votes == _False) _False else _True
      Message(votes = votes)
    }else { // si le receveur est un etat
      val stop_signal = if(message1.stopSignal == _True || message2.stopSignal == _True) _True else _False
      val blockIdCollection = message1.blockIdCollection.union(message2.blockIdCollection)
      Message(blockIdCollection, null, stop_signal)
    }
  }

  def main(args: Array[String]): Unit = {

  """----------------------------------------------------------------------------------------------------------------"""
              """ La Partie Configuration """
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "AutomataMinimizationPregel")

  """----------------------------------------------------------------------------------------------------------------"""
              """ Le Programme """

    val transitionsRDD = sc.makeRDD(Seq(
      // les transitions
      (0, "a", 0),
      (0, "b", 3),
      (3, "a", 3),
      (3, "b", 1),
      (1, "a", 1),
      (1, "b", 2),
      (2, "a", 2),
      (2, "b", 3),

      // les arrete entrants et sortants du superviseur
      (supervisorId, null, 0),
      (0, null, supervisorId),
      (supervisorId, null, 1),
      (1, null, supervisorId),
      (supervisorId, null, 2),
      (2, null, supervisorId),
      (supervisorId, null, 3),
      (3, null, supervisorId),
    ))

    var stateBlockIdsRDD = sc.makeRDD(Seq(
      // les etats
      (0, "0"),
      (1, "0"),
      (2, "0"),
      (3, "1"),
      //le superviseur
      (supervisorId, "")
    ))

    val edges = transitionsRDD.map{case (src, alpha, dest) => Edge(src, dest, alpha)}
    val vertices = stateBlockIdsRDD
      .map{ case (stateId, blockId) =>
        var stateData:StateData = if(stateId == supervisorId) StateData() else StateData(BlockId(blockId,1))
        (stateId.toLong, stateData)
      }

    val graph: Graph[StateData, String] = Graph(vertices, edges)
    val gra2 = graph.pregel[Message](initialMessage, 10, EdgeDirection.In)(vProg, sendMsg, mergeMsg)

  """----------------------------------------------------------------------------------------------------------------"""
                """ L'Affichage """
    gra2.vertices.collect.foreach(println)

  }

  }
