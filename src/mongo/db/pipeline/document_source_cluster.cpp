/**
 *    Copyright (C) 2018-present MongoDB, Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the Server Side Public License, version 1,
 *    as published by MongoDB, Inc.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    Server Side Public License for more details.
 *
 *    You should have received a copy of the Server Side Public License
 *    along with this program. If not, see
 *    <http://www.mongodb.com/licensing/server-side-public-license>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the Server Side Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "mongo/platform/basic.h"

#include "mongo/db/pipeline/document_source_cluster.h"

#include "mongo/db/pipeline/accumulation_statement.h"
#include "mongo/db/pipeline/lite_parsed_document_source.h"
#include "mongo/util/log.h"

#include <cmath>

namespace mongo {

using boost::intrusive_ptr;
using std::pair;
using std::string;
using std::vector;

REGISTER_DOCUMENT_SOURCE(cluster,
                         LiteParsedDocumentSourceDefault::parse,
                         DocumentSourceCluster::createFromBson);


const char* DocumentSourceCluster::getSourceName() const {
    return "$cluster";
}

DocumentSource::GetNextResult DocumentSourceCluster::getNext() {
    pExpCtx->checkForInterrupt();
    LOG(3) << "getNext " ;

    if (!_populated) {
        pair<Value, Document> currentPair;
        Value currentValue;
        bool isBacketFound = false;
        auto next = pSource->getNext();
        for (; next.isAdvanced(); next = pSource->getNext()) {
            auto nextDoc = next.releaseDocument();
            currentValue = extractKey(nextDoc);
            currentPair = std::make_pair(currentValue, nextDoc);
            
            LOG(3) << "getNext first:" << currentValue;

            isBacketFound = findBucket(currentValue);
            if(!isBacketFound) {
              Bucket currentBucket(
                  pExpCtx, currentValue, _accumulatedFields);
              addDocumentToBucket(currentPair, currentBucket);
              addBucket(currentBucket);
            } else {
              Bucket& currentBucket = *(_bucketsIterator);
              addDocumentToBucket(currentPair, currentBucket);
            }
            
            _nDocuments++;
        }
        if (next.isPaused()) {
            return next;
        }
        invariant(next.isEOF());

        _populated = true;
        _bucketsIterator = _buckets.begin();
    }

    if (_bucketsIterator == _buckets.end()) {
        dispose();
        return GetNextResult::makeEOF();
    }
    LOG(3) << "makeDocument " ;
    return makeDocument(*(_bucketsIterator++));
}

DocumentSource::GetDepsReturn DocumentSourceCluster::getDependencies(DepsTracker* deps) const {
    // Add the 'groupBy' expression.
    _groupByExpression->addDependencies(deps);

    // Add the 'output' fields.
    for (auto&& accumulatedField : _accumulatedFields) {
        accumulatedField.expression->addDependencies(deps);
    }

    // We know exactly which fields will be present in the output document. Future stages cannot
    // depend on any further fields. The grouping process will remove any metadata from the
    // documents, so there can be no further dependencies on metadata.
    return EXHAUSTIVE_ALL;
}

Value DocumentSourceCluster::extractKey(const Document& doc) {
    if (!_groupByExpression) {
        return Value(BSONNULL);
    }

    Value key = _groupByExpression->evaluate(doc);
    
    LOG(3) << "key :" << key ;
    // TODO check if extracted value match delta
    uassert(40709,
                str::stream() << "$cluster  'groupBy' value type must match"
                              << " with delta type "
                              << typeName(_delta.getType())
                              << " but found a value with type: "
                              << typeName(key.getType()),
                key.getType() == _delta.getType());
    if(_delta.getType() == Array) {
        std::vector<Value> arrayDelta = _delta.getArray();
        std::vector<Value> arrayKey = key.getArray();
        uassert(40710,
                str::stream() << "$cluster  'groupBy' value size type must match"
                              << " with delta size "
                              << arrayDelta.size()
                              << " but found a value with type: "
                              << arrayKey.size(),
                arrayKey.size() == arrayDelta.size());
    }
    // To be consistent with the $group stage, we consider "missing" to be equivalent to null when
    // grouping values into buckets.
    return key.missing() ? Value(BSONNULL) : std::move(key);
}

bool DocumentSourceCluster::findBucket(const Value& entry) {
    const bool isNumeric = _delta.numeric();
    const bool isArray = (_delta.getType() == Array);
    if(_buckets.empty()) {
        return false;
    }
    //const bool mergingOutput = false;
    _bucketsIterator = _buckets.begin();
    while(_bucketsIterator != _buckets.end()){
        Bucket& currentBucket = *(_bucketsIterator);
        Value groupBy = currentBucket._groupBy;
        LOG(3) << "bucket groupBy " << groupBy;

        if(isNumeric) {
          LOG(3) << "isNumeric " ;
            Value buckDelta = abs(subtract(groupBy, entry));
            LOG(3) << "delta :" << buckDelta ;
            int cmp = pExpCtx->getValueComparator().compare(_delta, buckDelta);
            if(cmp != -1) {
                LOG(3) << "found :" << groupBy ;
                return true;
            }
        } else if (isArray) {
          LOG(3) << "isArray " ;
            std::vector<Value> arrayDelta = _delta.getArray();
            std::vector<Value> arrayGroupBy = groupBy.getArray();
            std::vector<Value> arrayEntry = entry.getArray();
            LOG(3) << "arrayDelta :" << _delta ;
            LOG(3) << "arrayGroupBy :" << groupBy ;
            LOG(3) << "arrayEntry :" << entry ;
            if(arrayDelta.size() == arrayGroupBy.size() && arrayGroupBy.size() == arrayEntry.size()){
                size_t startIndex = 0;
                size_t endIndex = arrayDelta.size();
                bool isFound = true;
                LOG(3) << "startIndex :" << startIndex ;
                LOG(3) << "endIndex :" << endIndex ;
                for (size_t i = startIndex; i < endIndex; i++) {
                    Value buckDelta = abs(subtract(arrayGroupBy[i], arrayEntry[i]));
                    LOG(3) << "delta :" << buckDelta ;
                    int cmp = pExpCtx->getValueComparator().compare(arrayDelta[i], buckDelta);
                    if(cmp == -1) {
                        LOG(3) << "miss :" << groupBy ;
                        isFound = false;
                        break;
                    }
                }
                if(isFound) {
                  LOG(3) << "found :" << groupBy ;
                  return true;
                }
            } else {
              LOG(3) << "Dif size arrayDelta :" << arrayDelta.size() ;
              LOG(3) << "Dif size arrayGroupBy :" << arrayGroupBy.size() ;
              LOG(3) << "Dif size arrayEntry :" << arrayEntry.size() ;
            }

        }
        _bucketsIterator++;
    }
    LOG(3) << "not found :" << entry ;
    return false;
}
Value DocumentSourceCluster::abs(const Value& numericArg) {
    BSONType type = numericArg.getType();
    if (type == NumberDouble) {
        return Value(std::abs(numericArg.getDouble()));
    } else if (type == NumberDecimal) {
        return Value(numericArg.getDecimal().toAbs());
    } else {
        long long num = numericArg.getLong();
        uassert(40708,
                "can't take $abs of long long min",
                num != std::numeric_limits<long long>::min());
        long long absVal = std::abs(num);
        return type == NumberLong ? Value(absVal) : Value::createIntOrLong(absVal);
    }
}
Value DocumentSourceCluster::subtract(const Value& lhs, const Value& rhs) {
    BSONType diffType = Value::getWidestNumeric(rhs.getType(), lhs.getType());

    if (diffType == NumberDecimal) {
        Decimal128 right = rhs.coerceToDecimal();
        Decimal128 left = lhs.coerceToDecimal();
        return Value(left.subtract(right));
    } else if (diffType == NumberDouble) {
        double right = rhs.coerceToDouble();
        double left = lhs.coerceToDouble();
        return Value(left - right);
    } else if (diffType == NumberLong) {
        long long right = rhs.coerceToLong();
        long long left = lhs.coerceToLong();
        return Value(left - right);
    } else if (diffType == NumberInt) {
        long long right = rhs.coerceToLong();
        long long left = lhs.coerceToLong();
        return Value::createIntOrLong(left - right);
    } else if (lhs.nullish() || rhs.nullish()) {
        return Value(BSONNULL);
    } else if (lhs.getType() == Date) {
        if (rhs.getType() == Date) {
            return Value(durationCount<Milliseconds>(lhs.getDate() - rhs.getDate()));
        } else if (rhs.numeric()) {
            return Value(lhs.getDate() - Milliseconds(rhs.coerceToLong()));
        } else {
            uasserted(40706,
                      str::stream() << "cant $subtract a " << typeName(rhs.getType())
                                    << " from a Date");
        }
    } else {
        uasserted(40707,
                  str::stream() << "cant $subtract a" << typeName(rhs.getType()) << " from a "
                                << typeName(lhs.getType()));
    }
}

void DocumentSourceCluster::addDocumentToBucket(const pair<Value, Document>& entry,
                                                Bucket& bucket) {
    const size_t numAccumulators = _accumulatedFields.size();
    for (size_t k = 0; k < numAccumulators; k++) {
        bucket._accums[k]->process(_accumulatedFields[k].expression->evaluate(entry.second), false);
    }
}

DocumentSourceCluster::Bucket::Bucket(const boost::intrusive_ptr<ExpressionContext>& expCtx,
                                      Value groupBy,
                                      const vector<AccumulationStatement>& accumulationStatements)
    : _groupBy(groupBy) {
    _accums.reserve(accumulationStatements.size());
    for (auto&& accumulationStatement : accumulationStatements) {
        _accums.push_back(accumulationStatement.makeAccumulator(expCtx));
    }
}

void DocumentSourceCluster::addBucket(Bucket& newBucket) {
    _buckets.push_back(newBucket);
}

Document DocumentSourceCluster::makeDocument(const Bucket& bucket) {
    const size_t nAccumulatedFields = _accumulatedFields.size();
    MutableDocument out(1 + nAccumulatedFields);

    out.addField("_id", bucket._groupBy);

    const bool mergingOutput = false;
    for (size_t i = 0; i < nAccumulatedFields; i++) {
        Value val = bucket._accums[i]->getValue(mergingOutput);

        // To be consistent with the $group stage, we consider "missing" to be equivalent to null
        // when evaluating accumulators.
        out.addField(_accumulatedFields[i].fieldName, val.missing() ? Value(BSONNULL) : std::move(val));
    }
    return out.freeze();
}

void DocumentSourceCluster::doDispose() {
    _sortedInput.reset();
    _bucketsIterator = _buckets.end();
}

Value DocumentSourceCluster::serialize(
    boost::optional<ExplainOptions::Verbosity> explain) const {
    MutableDocument insides;

    insides["groupBy"] = _groupByExpression->serialize(static_cast<bool>(explain));
    insides["delta"] = Value(_delta);


    MutableDocument outputSpec(_accumulatedFields.size());
    for (auto&& accumulatedField : _accumulatedFields) {
        intrusive_ptr<Accumulator> accum = accumulatedField.makeAccumulator(pExpCtx);
        outputSpec[accumulatedField.fieldName] =
            Value{Document{{accum->getOpName(),
                            accumulatedField.expression->serialize(static_cast<bool>(explain))}}};
    }
    insides["output"] = outputSpec.freezeToValue();

    return Value{Document{{getSourceName(), insides.freezeToValue()}}};
}


intrusive_ptr<DocumentSourceCluster> DocumentSourceCluster::create(
    const intrusive_ptr<ExpressionContext>& pExpCtx,
    const boost::intrusive_ptr<Expression>& groupExpression,
    Value Delta,
    std::vector<AccumulationStatement> accumulationStatements,
    uint64_t maxMemoryUsageBytes) {
    // If there is no output field specified, then add the default one.
    if (accumulationStatements.empty()) {
        accumulationStatements.emplace_back("count",
                                            ExpressionConstant::create(pExpCtx, Value(1)),
                                            AccumulationStatement::getFactory("$sum"));
    }
    return new DocumentSourceCluster(pExpCtx,
                                        groupExpression,
                                        Delta,
                                        accumulationStatements,
                                        maxMemoryUsageBytes);
}

DocumentSourceCluster::DocumentSourceCluster(
    const intrusive_ptr<ExpressionContext>& pExpCtx,
    const boost::intrusive_ptr<Expression>& groupExpression,
    Value Delta,
    std::vector<AccumulationStatement> accumulationStatements,
    uint64_t maxMemoryUsageBytes)
    : DocumentSource(pExpCtx),
      _delta(Delta),
      _maxMemoryUsageBytes(maxMemoryUsageBytes),
      _groupByExpression(groupExpression) {

    invariant(!accumulationStatements.empty());
    for (auto&& accumulationStatement : accumulationStatements) {
        _accumulatedFields.push_back(accumulationStatement);
    }
}

namespace {

boost::intrusive_ptr<Expression> parseGroupByExpression(
    const boost::intrusive_ptr<ExpressionContext>& expCtx,
    const BSONElement& groupByField,
    const VariablesParseState& vps) {
    if (groupByField.type() == BSONType::Object &&
        groupByField.embeddedObject().firstElementFieldName()[0] == '$') {
        return Expression::parseObject(expCtx, groupByField.embeddedObject(), vps);
    } else if (groupByField.type() == BSONType::String &&
               groupByField.valueStringData()[0] == '$') {
        return ExpressionFieldPath::parse(expCtx, groupByField.str(), vps);
    } else {
        uasserted(
            40705,
            str::stream() << "The $cluster 'groupBy' field must be defined as a $-prefixed "
                             "path or an expression object, but found: "
                          << groupByField.toString(false, false));
    }
}
}  // namespace

intrusive_ptr<DocumentSource> DocumentSourceCluster::createFromBson(
    BSONElement elem, const intrusive_ptr<ExpressionContext>& pExpCtx) {
    uassert(40700,
            str::stream() << "Argument to $cluster stage must be an object, but found type: "
                          << typeName(elem.type())
                          << ".",
            elem.type() == BSONType::Object);

    VariablesParseState vps = pExpCtx->variablesParseState;
    vector<AccumulationStatement> accumulationStatements;
    boost::intrusive_ptr<Expression> groupExpression;
    Value Delta;
    bool isDelta = false;

    for (auto&& argument : elem.Obj()) {
        const auto argName = argument.fieldNameStringData();
        if ("groupBy" == argName) {
          groupExpression = parseGroupByExpression(pExpCtx, argument, vps);
        }  else if ("delta" == argName) {
          Delta = Value(argument);
          if(!Delta.numeric()){
              uassert(
                  40701,
                  str::stream() << "The $cluster 'delta' field must be a numeric or array of numeric, but found type: "
                                << typeName(Delta.getType())
                                << ".",
                  Delta.getType() == Array);
              std::vector<Value> arrayDelta = Delta.getArray();
              size_t startIndex = 0;
              size_t endIndex = arrayDelta.size();
              for (size_t i = startIndex; i < endIndex; i++) {
                  uassert(
                      40711,
                      str::stream() << "The $cluster 'delta' array item must be a numeric,"               << "but found type: "
                                    << typeName(arrayDelta[i].getType())
                                    << ".",
                      arrayDelta[i].numeric());
              }
          }
          isDelta = true;
        } else if ("output" == argName) {
            uassert(
                40702,
                str::stream() << "The $cluster 'output' field must be an object, but found type: "
                              << typeName(argument.type())
                              << ".",
                argument.type() == BSONType::Object);

            for (auto&& outputField : argument.embeddedObject()) {
                accumulationStatements.push_back(
                    AccumulationStatement::parseAccumulationStatement(pExpCtx, outputField, vps));
            }
        } else {
            uasserted(40703, str::stream() << "Unrecognized option to $cluster: " << argName << ".");
        }
    }
    
    uassert(40704,
            "$cluster requires 'groupBy' and 'delta' to be specified",
            groupExpression && isDelta );

    return DocumentSourceCluster::create(pExpCtx,
                                            groupExpression,
                                            Delta,
                                            accumulationStatements);
}
}  // namespace mongo


#include "mongo/db/sorter/sorter.cpp"
// Explicit instantiation unneeded since we aren't exposing Sorter outside of this file.
