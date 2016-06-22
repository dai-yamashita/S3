import assert from 'assert';
import Promise from 'bluebird';

import { Testing } from 'arsenal';
import BucketUtility from '../../lib/utility/bucket-util';


describe('Bucket GET (object listing)', () => {
    let bucketName;
    let params = {
        'auth': [ 'v2', 'v4' ],
        'Bucket': [ undefined, 'invalid-bucket-name', 'test-get-bucket' ],
        'Delimiter': [  undefined, '/', '', '|' ],
        'Prefix': [ undefined, '/validPrefix/ThatIsNot/InTheSet',
        '/validPrefix/ThatIsPresent/InTheTest', 'InvalidPrefix',
        '/ThatIsPresent/validPrefix/InTheTest',
        '|validPrefix|ThatIsNot|InTheSet'],
        'MaxKeys': [ undefined, 0, -1, 42, 1001, 1000, "string" ],
        'EncodingType': [undefined, "url"],
    };

    before(done => {

        const bucketUtil = new BucketUtility('default');
        const s3 = bucketUtil.s3;

        bucketUtil.empty("test-get-bucket").then(() => {});

        const generateDataSet = () => {
            const Bucket = 'test-get-bucket';

            for (let i = 0; i != 1200; ++i) {
                const Key = '/validPrefix/ThatIsPresent/InTheTest/' + i.toString() + ' ' + '/' + '_key' + String.fromCharCode(1) + '_url_test';
                const objects = [
                    { Bucket, Key },
                ];

                Promise.mapSeries(objects, param => s3.putObjectAsync(param))
                .then(() => s3.putObjectAclAsync({ Bucket, Key, ACL: 'public-read' }));
            }

            for (let i = 0; i != 250; ++i) {
                const Key = '|validPrefix|ThatIsPresent|InTheTest|' + i.toString() + ' ' + '/' + '_key' + String.fromCharCode(1) + '_url_test';
                const objects = [
                    { Bucket, Key },
                ];

                Promise.mapSeries(objects, param => s3.putObjectAsync(param))
                .then(() => s3.putObjectAclAsync({ Bucket, Key, ACL: 'public-read' }));
            }

        };

        bucketUtil.createOne('test-get-bucket')
        .then(created => {
            generateDataSet();
            done();
        }).catch(() => {
            bucketUtil.empty("test-get-bucket").then(() => {
                generateDataSet();
                done();
            });
        });
    });

    after(done => {
        const bucketUtil = new BucketUtility('default');

        bucketUtil.empty('test-get-bucket').then(() => {
            bucketUtil.deleteOne('test-get-bucket').then(() => done()).catch(done);
        }).catch(done);
    });

    const matrix = new Testing.Matrix.TestMatrix(params);

    matrix.generate(['auth'], (matrix, done) => {
        let expectedStatus = true;
        if (matrix.params.auth == undefined) {
            expectedStatus = False;
        }

        matrix.generate(['Delimiter', 'Prefix', 'MaxKeys', 'EncodingType',
        'Bucket'], matrix => {
            const statusMsg = expectedStatus ? "fail" : "succeed";

            describe (`should ${statusMsg}`, () => {
                const bucketUtil = new BucketUtility('default');

                delete matrix.params.auth;

                it(matrix.toString(), (done) => {
                    bucketUtil.s3.listObjects(matrix.params, (err, data) => {
                        const maxNumberOfKeys = 1200 < matrix.params.MaxKeys ? 1200 : matrix.params.MaxKeys;
                        if (matrix.params.Delimiter === undefined
                            && matrix.params.Prefix === undefined && matrix.params.MaxKeys !== undefined) {
                                assert.equal(data.Contents.length <= maxNumberOfKeys, true);
                            }
                            assert.equal(err === null, true);
                            done();
                        });
                    });
                });
            }).if({Bucket: [undefined, 'invalid-bucket-name']}, matrix => {
                const bucketUtil = new BucketUtility('default');

                delete matrix.params.auth;
                it("invalid-bucket-name" + matrix.toString(), (done) => {
                    bucketUtil.s3.listObjects(matrix.params, (err, data) => {
                        assert.equal(err !== null, true);
                        done();
                    });
                });
            }).if({MaxKeys: [-1, "string"]}, matrix => {
                const bucketUtil = new BucketUtility('default');

                delete matrix.params.auth;
                it("invalid max key " + matrix.toString(), done => {
                    bucketUtil.s3.listObjects(matrix.params, (err, data) => {
                        assert.equal(err !== null, true);
                        done();
                    });
                });
            }).if({Bucket: ['test-get-bucket'], EncodingType: ['url'],
            MaxKeys: [1000, 42, 1001, 1], Delimiter: ['/'], Prefix: ['/validPrefix/ThatIsPresent/InTheTest']},
            matrix => {
                const bucketUtil = new BucketUtility('default');

                delete matrix.params.auth;

                it("url " + matrix.toString(), done => {
                    bucketUtil.s3.listObjects(matrix.params, (err, data) => {
                        assert.equal(err === null, true);
                        assert.equal(data.Contents !== null && data.Contents[0].Key.indexOf("%01") !== -1, true);
                    });
                });

            }).if({Bucket: ['test-get-bucket'],
            MaxKeys: [1000, 42, 1001, 1], Delimiter: ['|'], Prefix: ['|validPrefix|ThatIsPresent|InTheTest']},
            matrix => {
                const bucketUtil = new BucketUtility('default');
                const maxNumberOfKeys = matrix.params.MaxKeys < 250 ? 250 : matrix.params.MaxKeys;
                delete matrix.params.auth;

                it("custom delimiter " + matrix.toString(), done => {
                    bucketUtil.s3.listObjects(matrix.params, (err, data) => {
                        assert.equal(err === null, true);
                        assert.equal(data.Contents.length <= maxNumberOfKeys, true);
                    });
                });

            }).if({Bucket: ['test-get-bucket'],
            Delimiter: ['/'],
            Prefix: ['/validPrefix/ThatIsNot/InTheSet', 'InvalidPrefix',
            '/ThatIsPresent/validPrefix/InTheTest']}, matrix => {
                const bucketUtil = new BucketUtility('default');

                delete matrix.params.auth;
                it("Invalid prefix " + matrix.toString(), done => {
                    bucketUtil.s3.listObjects(matrix.params, (err, data) => {

                        assert.equal(err === null, true);
                        assert.equal(data.Contants.length === null
                            || data.Contents.length === 0, true);
                            done();
                        });
                    });
                }).if({Bucket: ['test-get-bucket'], auth: ['v4']}, matrix => {

                    const cfg = {
                        signatureVersion: 'v4',
                    };

                    const bucketUtil = new BucketUtility('default', cfg);

                    delete matrix.params.auth;

                    it("V4", (done) => {
                        bucketUtil.s3.listObjects(matrix.params, (err, data) => {
                            done();
                        });
                    });
                });
            }).execute();
        });
