Fixed an out-of-bounds read when deserializing a corrupt wide-column entity whose declared version 2 section sizes sum to more than 4 GiB; such an entity is now reported as `Status::Corruption`.
