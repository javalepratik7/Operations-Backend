const { Upload } = require('@aws-sdk/lib-storage');
const s3Client = require('../config/s3Client');

const uploadFileToS3 = async (file, ticketId) => {
  const upload = new Upload({
    client: s3Client, // ✅ MUST be S3Client
    params: {
      Bucket: process.env.AWS_S3_BUCKET,
      Key: `tickets/${ticketId}/${Date.now()}-${file.originalname}`,
      Body: file.buffer,
      ContentType: file.mimetype
    }
  });

  const result = await upload.done();
  return result.Location; // S3 URL
};

module.exports = { uploadFileToS3 };
