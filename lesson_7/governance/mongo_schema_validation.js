
db.createCollection("assessment_result_documents", {
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: [
        "id_assessment",
        "id_student",
        "is_banked"
      ],
      additionalProperties: false,
      properties: {
        _id: {
          bsonType: "objectId"
        },
        id_assessment: {
          bsonType: ["int", "long"],
          minimum: 1
        },
        id_student: {
          bsonType: ["int", "long"],
          minimum: 1
        },
        date_submitted: {
          bsonType: ["int", "long", "null"]
        },
        is_banked: {
          bsonType: ["int", "long"],
          enum: [0, 1]
        },
        score: {
          bsonType: ["double", "int", "long", "decimal", "null"],
          minimum: 0,
          maximum: 100
        },
        course: {
          bsonType: "object",
          required: [
            "code_module",
            "code_presentation"
          ],
          additionalProperties: false,
          properties: {
            code_module: {
              bsonType: "string",
              minLength: 1
            },
            code_presentation: {
              bsonType: "string",
              minLength: 1
            }
          }
        },
        metadata: {
          bsonType: "object"
        }
      }
    }
  },
  validationLevel: "strict",
  validationAction: "error"
});
