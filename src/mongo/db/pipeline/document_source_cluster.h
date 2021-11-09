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

#pragma once

#include "mongo/db/pipeline/accumulation_statement.h"
#include "mongo/db/pipeline/accumulator.h"
#include "mongo/db/pipeline/document_source.h"
#include "mongo/db/pipeline/expression.h"
#include "mongo/db/sorter/sorter.h"

namespace mongo {

/**
 * The $cluster stage takes user specified field with location and distance and group locations by it.
 */
class DocumentSourceCluster final : public DocumentSource, public SplittableDocumentSource {
public:
    Value serialize(boost::optional<ExplainOptions::Verbosity> explain = boost::none) const final;
    GetDepsReturn getDependencies(DepsTracker* deps) const final;
    GetNextResult getNext() final;
    const char* getSourceName() const final;

    StageConstraints constraints(Pipeline::SplitState pipeState) const final {
        return {StreamType::kBlocking,
                PositionRequirement::kNone,
                HostTypeRequirement::kNone,
                DiskUseRequirement::kWritesTmpData,
                FacetRequirement::kAllowed};
    }
    /**
     * The $cluster stage must be run on the merging shard.
     */
    boost::intrusive_ptr<DocumentSource> getShardSource() final {
        return nullptr;
    }
    std::list<boost::intrusive_ptr<DocumentSource>> getMergeSources() final {
        return {this};
    }

    static const uint64_t kDefaultMaxMemoryUsageBytes = 100 * 1024 * 1024;

    
    /**
     * Convenience method to create a $cluster stage.
     *
     * If 'accumulationStatements' is the empty vector, it will be filled in with the statement
     * 'count: {$sum: 1}'.
     */
    static boost::intrusive_ptr<DocumentSourceCluster> create(
        const boost::intrusive_ptr<ExpressionContext>& expCtx,
        const boost::intrusive_ptr<Expression>& groupExpression,
        Value Delta,
        std::vector<AccumulationStatement> accumulationStatements = {},
        uint64_t maxMemoryUsageBytes = kDefaultMaxMemoryUsageBytes);

    /**
     * Parses a $cluster stage from the user-supplied BSON.
     */
    static boost::intrusive_ptr<DocumentSource> createFromBson(
        BSONElement elem, const boost::intrusive_ptr<ExpressionContext>& pExpCtx);

protected:
    void doDispose() final;

private:
    DocumentSourceCluster(const boost::intrusive_ptr<ExpressionContext>& pExpCtx,
                          const boost::intrusive_ptr<Expression>& groupExpression,
                          Value Delta,
                          std::vector<AccumulationStatement> accumulationStatements,
                          uint64_t maxMemoryUsageBytes);

    // struct for holding information about a bucket.
    struct Bucket {
        Bucket(const boost::intrusive_ptr<ExpressionContext>& expCtx,
                Value groupBy,
                const std::vector<AccumulationStatement>& accumulationStatements);
        Value _groupBy;
        std::vector<boost::intrusive_ptr<Accumulator>> _accums;
    };

    /**
     * Computes the 'location' expression value for 'doc'.
     */
    Value extractKey(const Document& doc);

    /**
     * Adds the document in 'entry' to 'bucket' by updating the accumulators in 'bucket'.
     */
    void addDocumentToBucket(const std::pair<Value, Document>& entry, Bucket& bucket);

    /**
     * Adds 'newBucket' to _buckets and updates any boundaries if necessary.
     */
    void addBucket(Bucket& newBucket);

    /**
     * Find 'bucket' in _buckets based on document.
     */
    bool findBucket(const Value& entry);

    /**
     * Apply ABS on value.
     */
    Value abs(const Value& numericArg);

    /**
     * Calculate delta between two values by subtracting them.
     */
    Value subtract(const Value& lhs, const Value& rhs);

    /**
     * Makes a document using the information from bucket. This is what is returned when getNext()
     * is called.
     */
    Document makeDocument(const Bucket& bucket);

    std::unique_ptr<Sorter<Value, Document>> _sorter;
    std::unique_ptr<Sorter<Value, Document>::Iterator> _sortedInput;

    // _fieldNames contains the field names for the result documents, _accumulatorFactories contains
    // the accumulator factories for the result documents, and _expressions contains the common
    // expressions used by each instance of each accumulator in order to find the right-hand side of
    // what gets added to the accumulator. These three vectors parallel each other.
    std::vector<AccumulationStatement> _accumulatedFields;
    
    Value _delta;
    
    int _nBuckets = 0;

    uint64_t _maxMemoryUsageBytes;
    bool _populated = false;
    std::vector<Bucket> _buckets;
    std::vector<Bucket>::iterator _bucketsIterator;
    std::unique_ptr<Variables> _variables;
    boost::intrusive_ptr<Expression> _groupByExpression;
    long long _nDocuments = 0;
};

}  // namespace mongo
