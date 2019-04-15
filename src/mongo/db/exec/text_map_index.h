#pragma once

//#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "mongo/platform/basic.h"

#include <boost/intrusive_ptr.hpp>
#include <boost/multi_index/hashed_index.hpp>
#include <boost/multi_index/member.hpp>
#include <boost/multi_index/mem_fun.hpp>
#include <boost/multi_index/tag.hpp>
#include <boost/multi_index/sequenced_index.hpp>
#include <boost/multi_index/ordered_index.hpp>
#include <boost/multi_index_container.hpp>
#include <boost/container/small_vector.hpp>
#include <boost/optional.hpp>
#include <vector>

#include "mongo/base/string_data_comparator_interface.h"
#include "mongo/db/exec/plan_stage.h"
#include "mongo/db/record_id.h"
#include "mongo/stdx/functional.h"

namespace mongo {

using boost::multi_index_container;
using boost::multi_index::sequenced;
using boost::multi_index::hashed_unique;
using boost::multi_index::member;
using boost::multi_index::tag;
using boost::multi_index::indexed_by;
using boost::multi_index::ordered_non_unique;
using boost::multi_index::const_mem_fun;
using std::vector;

class TextMapIndex {

public:
    typedef boost::container::small_vector<double, 10> ScoreStorage;

    struct IndexData{
      IndexData(): wsid(WorkingSet::INVALID_ID), score(0.0), advanced(false), collected(false) {}
      IndexData(RecordId _recordId,
                WorkingSetID _wsid,
                double _score,
                double _predictScore,
                bool _advanced,
                ScoreStorage _scoreTerms,
                ScoreStorage _scorePredictTerms)
      : recordId(_recordId),
        wsid(_wsid),
        score(_score),
        predictScore(_predictScore),
        advanced(_advanced) {
          scoreTerms = _scoreTerms;
          scorePredictTerms = _scorePredictTerms;
        }
      IndexData(RecordId _recordId,
                WorkingSetID _wsid)
      : recordId(_recordId),
        wsid(_wsid) {}
      RecordId recordId;
      WorkingSetID wsid;
      double score;
      double predictScore;
      bool advanced;
      ScoreStorage scoreTerms;
      ScoreStorage scorePredictTerms;
      bool collected;

    };
    struct Records {};
    struct Score {};
    struct ScorePredict {};

    using IndexContainer = 
      multi_index_container<
      IndexData,
      indexed_by< // list of indexes
          hashed_unique<  //hashed index over 'l'
            tag<Records>, // give that index a name
            member<IndexData, RecordId, &IndexData::recordId>, // what will be the index's key
            RecordId::Hasher
          >,
          ordered_non_unique<  //ordered index over 'i1'
            tag<Score>, // give that index a name
            member<IndexData, double, &IndexData::score>,
            std::greater<double> // what will be the index's key
          >,
          ordered_non_unique<  //ordered index over 'i1'
            tag<ScorePredict>, // give that index a name
            member<IndexData, double, &IndexData::predictScore>,
            std::greater<double> // what will be the index's key
          >
      >
    >;

    typedef IndexContainer::index<Score>::type ScoreIndex;
    typedef IndexContainer::index<ScorePredict>::type ScorePredictIndex;
    typedef IndexContainer::index<Records>::type RecordIndex;
    
    RecordIndex::iterator findByID(const RecordId& recordId) {
      return boost::multi_index::get<Records>(_container).find(recordId);
    };

    ScoreIndex::iterator scoreIterator(){
      return _scoreIterator;
    };

    ScoreIndex::iterator endScore() {
      return boost::multi_index::get<Score>(_container).end();
    };
    ScoreIndex::iterator beginScore() {
      return boost::multi_index::get<Score>(_container).begin();
    };

    ScorePredictIndex::iterator beginScorePredict() {
      return boost::multi_index::get<ScorePredict>(_container).begin();
    };
    ScorePredictIndex::iterator endScorePredict() {
      return boost::multi_index::get<ScorePredict>(_container).end();
    };

    bool isScoreEmpty() {
      if(_scoreIterator == boost::multi_index::get<Score>(_container).end()) {
        return true;
      }
      return false;
    };
    
    RecordIndex::iterator endRecords() {
      return boost::multi_index::get<Records>(_container).end();
    };

    struct refreshScoreAction{
      refreshScoreAction(std::vector<double> scoreStatus) {
        _scoreStatus = scoreStatus;
      }

      void operator()(IndexData& record)
      {

        record.score = 0;
        record.predictScore = 0;
        if(record.advanced) {
          return;
        }

        for (size_t i = 0; i < record.scoreTerms.size(); ++i) {
          record.score += record.scoreTerms[i];
          if(0 == record.scoreTerms[i]) {
            record.scorePredictTerms[i] = _scoreStatus[i];
          } else {
            record.scorePredictTerms[i] = record.scoreTerms[i];
          }
          record.predictScore += record.scorePredictTerms[i];
        }
      }

      private:
        std::vector<double> _scoreStatus;
    };

    struct updateScore {
      updateScore(size_t termID, double newScore, std::vector<double> scoreStatus):termID(termID), newScore(newScore){
        _scoreStatus = scoreStatus;
      }

      void operator()(IndexData& record)
      {
        /*std::cout << "\n before update ";
        std::cout << "\n update ";
        std::cout << "\n scoreTerms " << record.scoreTerms.size();
        std::cout << "\n scorePredictTerms " << record.scorePredictTerms.size();*/
        record.scoreTerms[termID] = newScore;
        if(record.advanced) {
          record.predictScore = 0;
          record.score = 0;
          return;
        }
        //std::cout << "\n update 1 ";
        record.score += newScore;
        record.predictScore = 0;
        for (size_t i = 0; i < record.scorePredictTerms.size(); ++i) {
          if(0 == record.scoreTerms[i]) {
            record.scorePredictTerms[i] = _scoreStatus[i];
          } else {
            record.scorePredictTerms[i] = record.scoreTerms[i];
          }
          record.predictScore += record.scorePredictTerms[i];
        }
        //std::cout << "\n update 2 ";
      }

      private:
        size_t termID;
        double newScore;
        std::vector<double> _scoreStatus;
    };

    struct trueAdvance {
      trueAdvance(bool advanced):advanced(advanced){}

      void operator()(IndexData& record)
      {
        record.advanced = advanced;
        record.predictScore = 0;
        record.score = 0;
      }
      private:
        bool advanced;
    };

    void update(RecordIndex::iterator it, size_t termID, double newScore, std::vector<double> scoreStatus) {
      //std::cout << "\n update " << termID << " score " << newScore;
      _container.modify(it, updateScore(termID, newScore, scoreStatus));
    };

    void refreshScore(const RecordId& recordId, std::vector<double> scoreStatus) {
      RecordIndex::iterator it = findByID(recordId);
      _container.modify(it, refreshScoreAction(scoreStatus));
    };

    void setAdvanced(const RecordId& recordId) {
      RecordIndex::iterator it = findByID(recordId);
      //std::cout << "\n set advanced  recordId " << recordId;
      _container.modify(it, trueAdvance(true));
    };

    void resetScopeIterator() {
      _scoreIterator = boost::multi_index::get<Score>(_container).begin();
    }

    IndexData getScore(){
      return *_scoreIterator;
    }

    void scoreStepBack(){
      --_scoreIterator;
    }
    void scoreStepForward(){
      ++_scoreIterator;
    }
    

    IndexData nextScore(){
      ++_scoreIterator;
      if(_scoreIterator == boost::multi_index::get<Score>(_container).end()) {
        return IndexData();
      }
      return *_scoreIterator;
    }

    void insert(IndexData data) {
      //std::cout << "\n insert " << data.recordId << " score " << data.score;
      _container.insert(data);
      if(1 == _container.size()) {
        // Force to set to beggining on first record;
        _scoreIterator = boost::multi_index::get<Score>(_container).begin();
      }
    }
    void erase(TextMapIndex::RecordIndex::iterator itC) {
        if (_container.empty()) {
            return;
        }
        _container.erase(itC);
    }

    void reserve(size_t m) {
      _container.reserve(m);
    }

    template <typename... Args>
    void emplace(Args&&... args){
      _container.emplace(std::forward<Args>(args)...);
    }

    /**
     * Returns the number of elements in the cache.
     */
    size_t size() const {
        return _container.size();
    }


private:
    IndexContainer _container;
    ScoreIndex::iterator _scoreIterator;
};
}
