pub struct ReadDescriptor;
pub struct Record;
pub fn read_batch(_reads: &[ReadDescriptor]) -> crate::Result<Vec<Record>> {
    Ok(Vec::new())
}
