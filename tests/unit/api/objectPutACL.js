import assert from 'assert';

import objectPut from '../../../lib/api/objectPut';
import bucketPut from '../../../lib/api/bucketPut';
import objectPutACL from '../../../lib/api/objectPutACL';
import metadata from '../../../lib/metadata/wrapper';
import utils from '../../../lib/utils';

const accessKey = 'accessKey1';
const namespace = 'default';
const bucketName = 'bucketname';
const postBody = [ new Buffer('I am a body'), ];
const testBucketUID = utils.getResourceUID(namespace, bucketName);

describe('putObjectACL API', () => {
    let metastore;
    beforeEach((done) => {
        metastore = {
            "users": {
                "accessKey1": {
                    "buckets": []
                },
                "accessKey2": {
                    "buckets": []
                }
            },
            "buckets": {}
        };
        metadata.deleteBucket(testBucketUID, ()=> {
            done();
        });
    });

    after((done) => {
        metadata.deleteBucket(testBucketUID, ()=> {
            done();
        });
    });

    it('should return an error if invalid canned ACL provided', (done) => {
        const correctMD5 = 'vnR+tLdVF79rPPfF+7YvOg==';
        const objectName = 'objectName';
        const testPutBucketRequest = {
            lowerCaseHeaders: {},
            headers: {
                host: `${bucketName}.s3.amazonaws.com`
            },
            url: '/',
            namespace: namespace
        };
        const testPutObjectRequest = {
            lowerCaseHeaders: {},
            url: `/${bucketName}/${objectName}`,
            namespace,
            post: postBody,
            calculatedMD5: 'vnR+tLdVF79rPPfF+7YvOg=='
        };

        const testObjACLRequest = {
            lowerCaseHeaders: {
                'x-amz-acl': 'invalid-option'
            },
            headers: {
                'x-amz-acl': 'invalid-option'
            },
            url: `/${bucketName}/${objectName}?acl`,
            namespace,
            query: {
                acl: ''
            }
        };

        bucketPut(accessKey, metastore, testPutBucketRequest,
            (err, success) => {
                assert.strictEqual(success, 'Bucket created');
                objectPut(accessKey, metastore,
                    testPutObjectRequest, (err, result) => {
                        assert.strictEqual(result, correctMD5);
                        objectPutACL(accessKey, metastore, testObjACLRequest,
                            (err) => {
                                assert.strictEqual(err, 'InvalidArgument');
                                done();
                            }
                        );
                    }
                );
            }
        );
    });

    it('should set a canned public-read-write ACL', (done) => {
        const correctMD5 = 'vnR+tLdVF79rPPfF+7YvOg==';
        const objectName = 'objectName';
        const testPutBucketRequest = {
            lowerCaseHeaders: {},
            headers: {
                host: `${bucketName}.s3.amazonaws.com`
            },
            url: '/',
            namespace: namespace
        };
        const testPutObjectRequest = {
            lowerCaseHeaders: {},
            url: `/${bucketName}/${objectName}`,
            namespace,
            post: postBody,
            calculatedMD5: 'vnR+tLdVF79rPPfF+7YvOg=='
        };

        const testObjACLRequest = {
            lowerCaseHeaders: {
                'x-amz-acl': 'public-read-write'
            },
            headers: {
                'x-amz-acl': 'public-read-write'
            },
            url: `/${bucketName}/${objectName}?acl`,
            namespace,
            query: {
                acl: ''
            }
        };

        bucketPut(accessKey, metastore, testPutBucketRequest,
            (err, success) => {
                assert.strictEqual(success, 'Bucket created');
                objectPut(accessKey, metastore,
                    testPutObjectRequest, (err, result) => {
                        assert.strictEqual(result, correctMD5);
                        objectPutACL(accessKey, metastore, testObjACLRequest,
                            (err) => {
                                assert.strictEqual(err, undefined);
                                metadata.getBucket(testBucketUID, (err, md) => {
                                    assert.strictEqual(md.keyMap
                                        .objectName.acl.Canned,
                                        'public-read-write');
                                    done();
                                });
                            }
                        );
                    }
                );
            }
        );
    });

    it('should set a canned public-read ACL followed by'
        + ' a canned authenticated-read ACL', (done) => {
        const correctMD5 = 'vnR+tLdVF79rPPfF+7YvOg==';
        const objectName = 'objectName';
        const testPutBucketRequest = {
            lowerCaseHeaders: {},
            headers: {
                host: `${bucketName}.s3.amazonaws.com`
            },
            url: '/',
            namespace: namespace
        };
        const testPutObjectRequest = {
            lowerCaseHeaders: {},
            url: `/${bucketName}/${objectName}`,
            namespace,
            post: postBody,
            calculatedMD5: 'vnR+tLdVF79rPPfF+7YvOg=='
        };

        const testObjACLRequest1 = {
            lowerCaseHeaders: {
                'x-amz-acl': 'public-read'
            },
            headers: {
                'x-amz-acl': 'public-read'
            },
            url: `/${bucketName}/${objectName}?acl`,
            namespace,
            query: {
                acl: ''
            }
        };

        const testObjACLRequest2 = {
            lowerCaseHeaders: {
                'x-amz-acl': 'authenticated-read'
            },
            headers: {
                'x-amz-acl': 'authenticated-read'
            },
            url: `/${bucketName}/${objectName}?acl`,
            namespace,
            query: {
                acl: ''
            }
        };

        bucketPut(accessKey, metastore, testPutBucketRequest,
            (err, success) => {
                assert.strictEqual(success, 'Bucket created');
                objectPut(accessKey, metastore,
                    testPutObjectRequest, (err, result) => {
                        assert.strictEqual(result, correctMD5);
                        objectPutACL(accessKey, metastore, testObjACLRequest1,
                            (err) => {
                                assert.strictEqual(err, undefined);
                                metadata.getBucket(testBucketUID, (err, md) => {
                                    assert.strictEqual(md.keyMap.objectName
                                        .acl.Canned, 'public-read');
                                    objectPutACL(accessKey, metastore,
                                        testObjACLRequest2,
                                        (err) => {
                                            assert.strictEqual(err, undefined);
                                            metadata.getBucket(testBucketUID,
                                                (err, md) => {
                                                    assert.strictEqual(md.keyMap
                                                    .objectName.acl.Canned,
                                                    'authenticated-read');
                                                    done();
                                                });
                                        });
                                });
                            }
                        );
                    }
                );
            }
        );
    });

    it('should set ACLs provided in request headers', (done) => {
        const canonicalIDforSample1 =
            '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be';
        const canonicalIDforSample2 =
            '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2bf';
        const correctMD5 = 'vnR+tLdVF79rPPfF+7YvOg==';
        const objectName = 'objectName';
        const testPutBucketRequest = {
            lowerCaseHeaders: {},
            headers: {
                host: `${bucketName}.s3.amazonaws.com`
            },
            url: '/',
            namespace: namespace
        };
        const testPutObjectRequest = {
            lowerCaseHeaders: {},
            url: `/${bucketName}/${objectName}`,
            namespace,
            post: postBody,
            calculatedMD5: 'vnR+tLdVF79rPPfF+7YvOg=='
        };

        const testObjACLRequest = {
            lowerCaseHeaders: {
                'x-amz-grant-full-control':
                    'emailaddress="sampleaccount1@sampling.com"' +
                    ',emailaddress="sampleaccount2@sampling.com"',
                'x-amz-grant-read':
                    'uri="http://acs.amazonaws.com/groups/s3/LogDelivery"',
                'x-amz-grant-read-acp':
                    'id="79a59df900b949e55d96a1e698fbacedfd6e09d98eac' +
                    'f8f8d5218e7cd47ef2be"',
                'x-amz-grant-write-acp':
                    'id="79a59df900b949e55d96a1e698fbacedfd6e09d98eac' +
                    'f8f8d5218e7cd47ef2bf"',
            },
            headers: {
                'x-amz-grant-full-control':
                    'emailaddress="sampleaccount1@sampling.com"' +
                    ',emailaddress="sampleaccount2@sampling.com"',
                'x-amz-grant-read':
                    'uri="http://acs.amazonaws.com/groups/s3/LogDelivery"',
                'x-amz-grant-read-acp':
                    'id="79a59df900b949e55d96a1e698fbacedfd6e09d98eac' +
                    'f8f8d5218e7cd47ef2be"',
                'x-amz-grant-write-acp':
                    'id="79a59df900b949e55d96a1e698fbacedfd6e09d98eac' +
                    'f8f8d5218e7cd47ef2bf"',
            },
            url: `/${bucketName}/${objectName}?acl`,
            namespace: namespace,
            query: {
                acl: ''
            }
        };

        bucketPut(accessKey, metastore, testPutBucketRequest,
            (err, success) => {
                assert.strictEqual(success, 'Bucket created');
                objectPut(accessKey, metastore,
                    testPutObjectRequest, (err, result) => {
                        assert.strictEqual(result, correctMD5);
                        objectPutACL(accessKey, metastore, testObjACLRequest,
                            (err) => {
                                assert.strictEqual(err, undefined);
                                metadata.getBucket(testBucketUID, (err, md) => {
                                    assert.strictEqual(md.keyMap.objectName
                                        .acl.READ[0],
                                        'http://acs.amazonaws.com/' +
                                        'groups/s3/LogDelivery');
                                    assert(md.keyMap.objectName
                                        .acl.FULL_CONTROL[0]
                                        .indexOf(canonicalIDforSample1) > -1);
                                    assert(md.keyMap.objectName
                                        .acl.FULL_CONTROL[1]
                                        .indexOf(canonicalIDforSample2) > -1);
                                    assert(md.keyMap.objectName
                                        .acl.READ_ACP[0]
                                        .indexOf(canonicalIDforSample1) > -1);
                                    assert(md.keyMap.objectName
                                        .acl.WRITE_ACP[0]
                                        .indexOf(canonicalIDforSample2) > -1);
                                    done();
                                });
                            });
                    }
                );
            }
        );
    });

    it('should return an error if invalid email ' +
        'provided in ACL header request', (done) => {
        const correctMD5 = 'vnR+tLdVF79rPPfF+7YvOg==';
        const objectName = 'objectName';
        const testPutBucketRequest = {
            lowerCaseHeaders: {},
            headers: {
                host: `${bucketName}.s3.amazonaws.com`
            },
            url: '/',
            namespace: namespace
        };
        const testPutObjectRequest = {
            lowerCaseHeaders: {},
            url: `/${bucketName}/${objectName}`,
            namespace,
            post: postBody,
            calculatedMD5: 'vnR+tLdVF79rPPfF+7YvOg=='
        };

        const testObjACLRequest = {
            lowerCaseHeaders: {
                'x-amz-grant-full-control':
                    'emailaddress="sampleaccount1@sampling.com"' +
                    ',emailaddress="nonexistentemail@sampling.com"'
            },
            headers: {
                'x-amz-grant-full-control':
                    'emailaddress="sampleaccount1@sampling.com"' +
                    ',emailaddress="nonexistentemail@sampling.com"'
            },
            url: `/${bucketName}/${objectName}?acl`,
            namespace: namespace,
            query: {
                acl: ''
            }
        };

        bucketPut(accessKey, metastore, testPutBucketRequest,
            (err, success) => {
                assert.strictEqual(success, 'Bucket created');
                objectPut(accessKey, metastore,
                    testPutObjectRequest, (err, result) => {
                        assert.strictEqual(result, correctMD5);
                        objectPutACL(accessKey, metastore, testObjACLRequest,
                            (err) => {
                                assert.strictEqual(err,
                                    'UnresolvableGrantByEmailAddress');
                                done();
                            }
                        );
                    }
                );
            }
        );
    });

    it('should set ACLs provided in request body', (done) => {
        const correctMD5 = 'vnR+tLdVF79rPPfF+7YvOg==';
        const objectName = 'objectName';
        const testPutBucketRequest = {
            lowerCaseHeaders: {},
            headers: {
                host: `${bucketName}.s3.amazonaws.com`
            },
            url: '/',
            namespace: namespace
        };
        const testPutObjectRequest = {
            lowerCaseHeaders: {},
            url: `/${bucketName}/${objectName}`,
            namespace,
            post: postBody,
            calculatedMD5: 'vnR+tLdVF79rPPfF+7YvOg=='
        };

        const testObjACLRequest = {
            lowerCaseHeaders: {},
            headers: {},
            url: `/${bucketName}/${objectName}?acl`,
            namespace: namespace,
            post: {
                '<AccessControlPolicy xmlns':
                    '"http://s3.amazonaws.com/doc/2006-03-01/">' +
                  '<Owner>' +
                    '<ID>852b113e7a2f25102679df27bb0ae12b3f85be6</ID>' +
                    '<DisplayName>OwnerDisplayName</DisplayName>' +
                  '</Owner>' +
                  '<AccessControlList>' +
                    '<Grant>' +
                      '<Grantee xsi:type="CanonicalUser">' +
                        '<ID>852b113e7a2f25102679df27bb0ae12b3f85be6</ID>' +
                        '<DisplayName>OwnerDisplayName</DisplayName>' +
                      '</Grantee>' +
                      '<Permission>FULL_CONTROL</Permission>' +
                    '</Grant>' +
                    '<Grant>' +
                      '<Grantee xsi:type="Group">' +
                        '<URI>http://acs.amazonaws.com/groups/' +
                        'global/AllUsers</URI>' +
                      '</Grantee>' +
                      '<Permission>READ</Permission>' +
                    '</Grant>' +
                    '<Grant>' +
                      '<Grantee xsi:type="AmazonCustomerByEmail">' +
                        '<EmailAddress>sampleaccount1@sampling.com' +
                        '</EmailAddress>' +
                      '</Grantee>' +
                      '<Permission>WRITE_ACP</Permission>' +
                    '</Grant>' +
                    '<Grant>' +
                      '<Grantee xsi:type="CanonicalUser">' +
                        '<ID>f30716ab7115dcb44a5ef76e9d74b8e20567f63</ID>' +
                      '</Grantee>' +
                      '<Permission>READ_ACP</Permission>' +
                    '</Grant>' +
                  '</AccessControlList>' +
                '</AccessControlPolicy>'},
            query: {
                acl: ''
            }
        };
        const canonicalIDforSample1 =
            '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be';

        bucketPut(accessKey, metastore, testPutBucketRequest,
            (err, success) => {
                assert.strictEqual(success, 'Bucket created');
                objectPut(accessKey, metastore,
                    testPutObjectRequest, (err, result) => {
                        assert.strictEqual(result, correctMD5);
                        objectPutACL(accessKey, metastore, testObjACLRequest,
                            (err) => {
                                assert.strictEqual(err, undefined);
                                metadata.getBucket(testBucketUID, (err, md) => {
                                    assert.strictEqual(md.keyMap.objectName
                                        .acl.FULL_CONTROL[0],
                                        '852b113e7a2f25102679df27bb' +
                                        '0ae12b3f85be6');
                                    assert.strictEqual(md.keyMap.objectName
                                        .acl.READ[0],
                                        'http://acs.amazonaws.com/' +
                                        'groups/global/AllUsers');
                                    assert.strictEqual(md.keyMap.objectName
                                        .acl.WRITE_ACP[0],
                                        canonicalIDforSample1);
                                    assert.strictEqual(md.keyMap.objectName
                                        .acl.READ_ACP[0],
                                        'f30716ab7115dcb44a5e' +
                                        'f76e9d74b8e20567f63');
                                    done();
                                });
                            });
                    }
                );
            }
        );
    });

    it('should ignore if WRITE ACL permission is ' +
        'provided in request body', (done) => {
        const correctMD5 = 'vnR+tLdVF79rPPfF+7YvOg==';
        const objectName = 'objectName';
        const testPutBucketRequest = {
            lowerCaseHeaders: {},
            headers: {
                host: `${bucketName}.s3.amazonaws.com`
            },
            url: '/',
            namespace: namespace
        };
        const testPutObjectRequest = {
            lowerCaseHeaders: {},
            url: `/${bucketName}/${objectName}`,
            namespace,
            post: postBody,
            calculatedMD5: 'vnR+tLdVF79rPPfF+7YvOg=='
        };

        const testObjACLRequest = {
            lowerCaseHeaders: {},
            headers: {},
            url: `/${bucketName}/${objectName}?acl`,
            namespace: namespace,
            post: {
                '<AccessControlPolicy xmlns':
                    '"http://s3.amazonaws.com/doc/2006-03-01/">' +
                  '<Owner>' +
                    '<ID>852b113e7a2f25102679df27bb0ae12b3f85be6</ID>' +
                    '<DisplayName>OwnerDisplayName</DisplayName>' +
                  '</Owner>' +
                  '<AccessControlList>' +
                    '<Grant>' +
                      '<Grantee xsi:type="CanonicalUser">' +
                        '<ID>852b113e7a2f25102679df27bb0ae12b3f85be6</ID>' +
                        '<DisplayName>OwnerDisplayName</DisplayName>' +
                      '</Grantee>' +
                      '<Permission>FULL_CONTROL</Permission>' +
                    '</Grant>' +
                    '<Grant>' +
                      '<Grantee xsi:type="Group">' +
                        '<URI>http://acs.amazonaws.com/groups/' +
                        'global/AllUsers</URI>' +
                      '</Grantee>' +
                      '<Permission>WRITE</Permission>' +
                    '</Grant>' +
                  '</AccessControlList>' +
                '</AccessControlPolicy>'},
            query: {
                acl: ''
            }
        };

        bucketPut(accessKey, metastore, testPutBucketRequest,
            (err, success) => {
                assert.strictEqual(success, 'Bucket created');
                objectPut(accessKey, metastore,
                    testPutObjectRequest, (err, result) => {
                        assert.strictEqual(result, correctMD5);
                        objectPutACL(accessKey, metastore, testObjACLRequest,
                            (err) => {
                                assert.strictEqual(err, undefined);
                                metadata.getBucket(testBucketUID, (err, md) => {
                                    assert.strictEqual(md.keyMap
                                        .objectName.acl.Canned, '');
                                    assert.strictEqual(md.keyMap
                                        .objectName.acl.FULL_CONTROL[0],
                                        '852b113e7a2f2510267' +
                                        '9df27bb0ae12b3f85be6');
                                    assert.strictEqual(md.keyMap
                                        .objectName.acl.WRITE, undefined);
                                    assert.strictEqual(md.keyMap
                                        .objectName.acl.READ[0], undefined);
                                    assert.strictEqual(md.keyMap
                                        .objectName.acl.WRITE_ACP[0],
                                        undefined);
                                    assert.strictEqual(md.keyMap
                                        .objectName.acl.READ_ACP[0], undefined);
                                    done();
                                });
                            });
                    }
                );
            }
        );
    });

    it('should return an error if invalid email ' +
    'address provided in ACLs set out in request body', (done) => {
        const correctMD5 = 'vnR+tLdVF79rPPfF+7YvOg==';
        const objectName = 'objectName';
        const testPutBucketRequest = {
            lowerCaseHeaders: {},
            headers: {
                host: `${bucketName}.s3.amazonaws.com`
            },
            url: '/',
            namespace: namespace
        };
        const testPutObjectRequest = {
            lowerCaseHeaders: {},
            url: `/${bucketName}/${objectName}`,
            namespace,
            post: postBody,
            calculatedMD5: 'vnR+tLdVF79rPPfF+7YvOg=='
        };

        const testObjACLRequest = {
            lowerCaseHeaders: {},
            headers: {},
            url: `/${bucketName}/${objectName}?acl`,
            namespace: namespace,
            post: {
                '<AccessControlPolicy xmlns':
                    '"http://s3.amazonaws.com/doc/2006-03-01/">' +
                  '<Owner>' +
                    '<ID>852b113e7a2f25102679df27bb0ae12b3f85be6</ID>' +
                    '<DisplayName>OwnerDisplayName</DisplayName>' +
                  '</Owner>' +
                  '<AccessControlList>' +
                    '<Grant>' +
                      '<Grantee xsi:type="AmazonCustomerByEmail">' +
                        '<EmailAddress>xyz@amazon.com' +
                        '</EmailAddress>' +
                      '</Grantee>' +
                      '<Permission>WRITE_ACP</Permission>' +
                    '</Grant>' +
                  '</AccessControlList>' +
                '</AccessControlPolicy>'},
            query: {
                acl: ''
            }
        };


        bucketPut(accessKey, metastore, testPutBucketRequest,
            (err, success) => {
                assert.strictEqual(success, 'Bucket created');
                objectPut(accessKey, metastore,
                    testPutObjectRequest, (err, result) => {
                        assert.strictEqual(result, correctMD5);
                        objectPutACL(accessKey, metastore, testObjACLRequest,
                            (err) => {
                                assert.strictEqual(err,
                                    'UnresolvableGrantByEmailAddress');
                                done();
                            }
                        );
                    }
                );
            }
        );
    });

    it('should return an error if xml provided does not match s3 ' +
    'scheme for setting ACLs', (done) => {
        const correctMD5 = 'vnR+tLdVF79rPPfF+7YvOg==';
        const objectName = 'objectName';
        const testPutBucketRequest = {
            lowerCaseHeaders: {},
            headers: {
                host: `${bucketName}.s3.amazonaws.com`
            },
            url: '/',
            namespace: namespace
        };
        const testPutObjectRequest = {
            lowerCaseHeaders: {},
            url: `/${bucketName}/${objectName}`,
            namespace,
            post: postBody,
            calculatedMD5: 'vnR+tLdVF79rPPfF+7YvOg=='
        };

        const testObjACLRequest = {
            lowerCaseHeaders: {},
            headers: {},
            url: `/${bucketName}/${objectName}?acl`,
            namespace: namespace,
            post: {
                '<AccessControlPolicy xmlns':
                    '"http://s3.amazonaws.com/doc/2006-03-01/">' +
                  '<Owner>' +
                    '<ID>852b113e7a2f25102679df27bb0ae12b3f85be6</ID>' +
                    '<DisplayName>OwnerDisplayName</DisplayName>' +
                  '</Owner>' +
                  '<AccessControlList>' +
                    '<PowerGrant>' +
                      '<Grantee xsi:type="AmazonCustomerByEmail">' +
                        '<EmailAddress>xyz@amazon.com' +
                        '</EmailAddress>' +
                      '</Grantee>' +
                      '<Permission>WRITE_ACP</Permission>' +
                    '</PowerGrant>' +
                  '</AccessControlList>' +
                '</AccessControlPolicy>'},
            query: {
                acl: ''
            }
        };


        bucketPut(accessKey, metastore, testPutBucketRequest,
            (err, success) => {
                assert.strictEqual(success, 'Bucket created');
                objectPut(accessKey, metastore,
                    testPutObjectRequest, (err, result) => {
                        assert.strictEqual(result, correctMD5);
                        objectPutACL(accessKey, metastore, testObjACLRequest,
                            (err) => {
                                assert.strictEqual(err, 'MalformedACLError');
                                done();
                            }
                        );
                    }
                );
            }
        );
    });

    it('should return an error if malformed xml provided', (done) => {
        const correctMD5 = 'vnR+tLdVF79rPPfF+7YvOg==';
        const objectName = 'objectName';
        const testPutBucketRequest = {
            lowerCaseHeaders: {},
            headers: {
                host: `${bucketName}.s3.amazonaws.com`
            },
            url: '/',
            namespace: namespace
        };
        const testPutObjectRequest = {
            lowerCaseHeaders: {},
            url: `/${bucketName}/${objectName}`,
            namespace,
            post: postBody,
            calculatedMD5: 'vnR+tLdVF79rPPfF+7YvOg=='
        };

        const testObjACLRequest = {
            lowerCaseHeaders: {},
            headers: {},
            url: `/${bucketName}/${objectName}?acl`,
            namespace: namespace,
            post: {
                '<AccessControlPolicy xmlns':
                    '"http://s3.amazonaws.com/doc/2006-03-01/">' +
                  '<Owner>' +
                    '<ID>852b113e7a2f25102679df27bb0ae12b3f85be6</ID>' +
                    '<DisplayName>OwnerDisplayName</DisplayName>' +
                  '</Owner>' +
                  '<AccessControlList>' +
                    '<Grant>' +
                      '<Grantee xsi:type="AmazonCustomerByEmail">' +
                        '<EmailAddress>xyz@amazon.com' +
                        '</EmailAddress>' +
                      '</Grantee>' +
                      '<Permission>WRITE_ACP</Permission>' +
                    '<Grant>' +
                  '</AccessControlList>' +
                '</AccessControlPolicy>'},
            query: {
                acl: ''
            }
        };


        bucketPut(accessKey, metastore, testPutBucketRequest,
            (err, success) => {
                assert.strictEqual(success, 'Bucket created');
                objectPut(accessKey, metastore,
                    testPutObjectRequest, (err, result) => {
                        assert.strictEqual(result, correctMD5);
                        objectPutACL(accessKey, metastore, testObjACLRequest,
                            (err) => {
                                assert.strictEqual(err, 'MalformedXML');
                                done();
                            }
                        );
                    }
                );
            }
        );
    });

    it('should return an error if invalid group ' +
    'uri provided in ACLs set out in request body', (done) => {
        const correctMD5 = 'vnR+tLdVF79rPPfF+7YvOg==';
        const objectName = 'objectName';
        const testPutBucketRequest = {
            lowerCaseHeaders: {},
            headers: {
                host: `${bucketName}.s3.amazonaws.com`
            },
            url: '/',
            namespace: namespace
        };
        const testPutObjectRequest = {
            lowerCaseHeaders: {},
            url: `/${bucketName}/${objectName}`,
            namespace,
            post: postBody,
            calculatedMD5: 'vnR+tLdVF79rPPfF+7YvOg=='
        };

        const testObjACLRequest = {
            lowerCaseHeaders: {},
            headers: {},
            url: `/${bucketName}/${objectName}?acl`,
            namespace: namespace,
            post: {
                '<AccessControlPolicy xmlns':
                    '"http://s3.amazonaws.com/doc/2006-03-01/">' +
                  '<Owner>' +
                    '<ID>852b113e7a2f25102679df27bb0ae12b3f85be6</ID>' +
                    '<DisplayName>OwnerDisplayName</DisplayName>' +
                  '</Owner>' +
                  '<AccessControlList>' +
                    '<Grant>' +
                    '<Grantee xsi:type="Group">' +
                      '<URI>http://acs.amazonaws.com/groups/' +
                      'global/NOTAVALIDGROUP</URI>' +
                    '</Grantee>' +
                      '<Permission>WRITE_ACP</Permission>' +
                    '<Grant>' +
                  '</AccessControlList>' +
                '</AccessControlPolicy>'},
            query: {
                acl: ''
            }
        };


        bucketPut(accessKey, metastore, testPutBucketRequest,
            (err, success) => {
                assert.strictEqual(success, 'Bucket created');
                objectPut(accessKey, metastore,
                    testPutObjectRequest, (err, result) => {
                        assert.strictEqual(result, correctMD5);
                        objectPutACL(accessKey, metastore, testObjACLRequest,
                            (err) => {
                                assert.strictEqual(err, 'MalformedXML');
                                done();
                            }
                        );
                    }
                );
            }
        );
    });

    it('should return an error if invalid group uri ' +
        'provided in ACL header request', (done) => {
        const correctMD5 = 'vnR+tLdVF79rPPfF+7YvOg==';
        const objectName = 'objectName';
        const testPutBucketRequest = {
            lowerCaseHeaders: {},
            headers: {
                host: `${bucketName}.s3.amazonaws.com`
            },
            url: '/',
            namespace: namespace
        };
        const testPutObjectRequest = {
            lowerCaseHeaders: {},
            url: `/${bucketName}/${objectName}`,
            namespace,
            post: postBody,
            calculatedMD5: 'vnR+tLdVF79rPPfF+7YvOg=='
        };

        const testObjACLRequest = {
            lowerCaseHeaders: {
                host: `s3.amazonaws.com`,
                'x-amz-grant-full-control':
                    'uri="http://acs.amazonaws.com/groups/' +
                    'global/NOTAVALIDGROUP"',
            },
            url: `/${bucketName}/${objectName}?acl`,
            namespace: namespace,
            headers: {
                host: `s3.amazonaws.com`,
                'x-amz-grant-full-control':
                    'uri="http://acs.amazonaws.com/groups/' +
                    'global/NOTAVALIDGROUP"',
            },
            query: {
                acl: ''
            }
        };


        bucketPut(accessKey, metastore, testPutBucketRequest,
            (err, success) => {
                assert.strictEqual(success, 'Bucket created');
                objectPut(accessKey, metastore,
                    testPutObjectRequest, (err, result) => {
                        assert.strictEqual(result, correctMD5);
                        objectPutACL(accessKey, metastore, testObjACLRequest,
                            (err) => {
                                assert.strictEqual(err, 'InvalidArgument');
                                done();
                            }
                        );
                    }
                );
            }
        );
    });
});